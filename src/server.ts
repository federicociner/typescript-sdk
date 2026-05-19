import { ConnectionRegistry } from "./connection.js";
import {
  EVENT_STREAM_MIME_TYPE,
  HEADER_CONNECTION_ID,
  HEADER_SESSION_ID,
  JSON_MIME_TYPE,
  isInitializeRequest,
  messageIdKey,
  methodRequiresSessionHeader,
  sessionIdFromParams,
} from "./protocol.js";
import {
  isJsonRpcMessage,
  isRequestMessage,
  isResponseMessage,
} from "./jsonrpc.js";
import { serializeSseEvent, serializeSseKeepAlive } from "./sse.js";
import { handleWebSocketConnection } from "./ws-server.js";
import type { WebSocketServerSocket } from "./ws-server.js";

import type {
  ConnectionState,
  OutboundSubscription,
  ResponseRoute,
} from "./connection.js";
import type { Agent, AgentSideConnection } from "./acp.js";
import type { AnyMessage, AnyRequest, AnyResponse } from "./jsonrpc.js";

/** Options for creating an ACP server transport. */
export interface AcpServerOptions {
  /** Creates the agent implementation for each accepted ACP connection. */
  createAgent: (conn: AgentSideConnection) => Agent;
}

/**
 * ACP server transport for Streamable HTTP and WebSocket connections.
 *
 * Route HTTP requests to {@link handleRequest}. For WebSocket upgrades, let your
 * framework perform the upgrade and pass the accepted socket to
 * {@link handleWebSocket}.
 */
export class AcpServer {
  private readonly createAgent: (conn: AgentSideConnection) => Agent;
  private readonly registry = new ConnectionRegistry();

  constructor(options: AcpServerOptions) {
    this.createAgent = options.createAgent;
  }

  /** Handles one Streamable HTTP ACP request. */
  async handleRequest(req: Request): Promise<Response> {
    if (req.method === "POST") {
      return await this.handlePost(req);
    }

    if (req.method === "GET") {
      return this.handleGet(req);
    }

    if (req.method === "DELETE") {
      return this.handleDelete(req);
    }

    return textResponse("Method Not Allowed", 405);
  }

  /** Handles one accepted ACP WebSocket connection. */
  handleWebSocket(socket: WebSocketServerSocket): void {
    handleWebSocketConnection(socket, {
      registry: this.registry,
      createAgent: this.createAgent,
    });
  }

  /** Closes all active ACP connections owned by this server. */
  async close(): Promise<void> {
    this.registry.closeAll();
  }

  private async handlePost(req: Request): Promise<Response> {
    const contentType = req.headers.get("Content-Type");

    if (!contentType?.startsWith(JSON_MIME_TYPE)) {
      return textResponse("Unsupported Media Type", 415);
    }

    const body = await readJson(req);

    if (!body.ok) {
      return textResponse("Invalid JSON", 400);
    }

    if (Array.isArray(body.value)) {
      return textResponse("Batch JSON-RPC requests are not implemented", 501);
    }

    if (!isJsonRpcMessage(body.value)) {
      return textResponse("Invalid JSON-RPC message", 400);
    }

    const connectionId = req.headers.get(HEADER_CONNECTION_ID);

    if (isInitializeRequest(body.value) && !connectionId) {
      return await this.handleInitialize(body.value);
    }

    if (!connectionId) {
      return textResponse("Missing Acp-Connection-Id", 400);
    }

    const connection = this.registry.get(connectionId);

    if (!connection) {
      return textResponse("Unknown Acp-Connection-Id", 404);
    }

    const forwarded = await this.forwardConnectedMessage(
      connection,
      body.value,
      req.headers,
    );
    if (!forwarded.ok) {
      return textResponse(forwarded.message, forwarded.status);
    }

    return emptyResponse(202);
  }

  private handleGet(req: Request): Response {
    if (req.headers.get("Upgrade")?.toLowerCase() === "websocket") {
      return textResponse("WebSocket upgrade is not implemented", 426);
    }

    const accept = req.headers.get("Accept")?.toLowerCase();

    if (!accept?.includes(EVENT_STREAM_MIME_TYPE)) {
      return textResponse("Not Acceptable", 406);
    }

    const connectionId = req.headers.get(HEADER_CONNECTION_ID);

    if (!connectionId) {
      return textResponse("Missing Acp-Connection-Id", 400);
    }

    const connection = this.registry.get(connectionId);

    if (!connection) {
      return textResponse("Unknown Acp-Connection-Id", 404);
    }

    const sessionId = req.headers.get(HEADER_SESSION_ID);
    if (sessionId) {
      const sessionStream = connection.sessionStreams.get(sessionId);
      if (!sessionStream) {
        return textResponse("Unknown Acp-Session-Id", 404);
      }

      return sseResponse(sessionStream.subscribe());
    }

    return sseResponse(connection.connectionStream.subscribe());
  }

  private handleDelete(req: Request): Response {
    const connectionId = req.headers.get(HEADER_CONNECTION_ID);

    if (!connectionId) {
      return textResponse("Missing Acp-Connection-Id", 400);
    }

    if (!this.registry.remove(connectionId)) {
      return textResponse("Unknown Acp-Connection-Id", 404);
    }

    return emptyResponse(202);
  }

  private async handleInitialize(message: AnyMessage): Promise<Response> {
    if (!("id" in message) || message.id === null) {
      return textResponse("Initialize request must include an ID", 400);
    }

    let connection:
      | ReturnType<ConnectionRegistry["createConnection"]>
      | undefined;

    try {
      connection = this.registry.createConnection(this.createAgent);
      await writeInbound(connection, message);

      const initialResponse = await connection.recvInitial(message.id);
      connection.startRouter();

      return jsonResponse(initialResponse, 200, {
        [HEADER_CONNECTION_ID]: connection.connectionId,
      });
    } catch (error) {
      if (connection) {
        this.registry.remove(connection.connectionId);
      }

      return jsonResponse(
        {
          jsonrpc: "2.0",
          id: message.id,
          error: {
            code: -32603,
            message: "Initialize failed",
            data: error instanceof Error ? error.message : undefined,
          },
        },
        500,
      );
    }
  }

  private async forwardConnectedMessage(
    connection: ConnectionState,
    message: AnyMessage,
    headers: Headers,
  ): Promise<ForwardResult> {
    if (isRequestMessage(message)) {
      return await forwardClientRequest(connection, message, headers);
    }

    if (isResponseMessage(message)) {
      return await forwardClientResponse(connection, message);
    }

    return await forwardClientNotification(connection, message);
  }
}

type ForwardResult =
  | {
      ok: true;
    }
  | {
      ok: false;
      status: number;
      message: string;
    };

type JsonResult =
  | {
      ok: true;
      value: unknown;
    }
  | {
      ok: false;
    };

type RouteResult =
  | {
      ok: true;
      value: ResponseRoute;
    }
  | {
      ok: false;
      status: number;
      message: string;
    };

type ClientRequestMessage = AnyRequest;

async function readJson(req: Request): Promise<JsonResult> {
  try {
    return {
      ok: true,
      value: await req.json(),
    };
  } catch {
    return {
      ok: false,
    };
  }
}

async function writeInbound(
  connection: ConnectionState,
  message: AnyMessage,
): Promise<void> {
  const writer = connection.inboundTx.getWriter();

  try {
    await writer.write(message);
  } finally {
    writer.releaseLock();
  }
}

async function forwardClientRequest(
  connection: ConnectionState,
  message: ClientRequestMessage,
  headers: Headers,
): Promise<ForwardResult> {
  const route = determineRoute(message, headers);

  if (!route.ok) {
    return route;
  }

  if (route.value !== "connection") {
    connection.ensureSession(route.value.session);
  }

  const key = messageIdKey(message.id);

  if (key) {
    connection.pendingRoutes.set(key, route.value);
  }

  await writeInbound(connection, message);
  return { ok: true };
}

async function forwardClientResponse(
  connection: ConnectionState,
  message: AnyResponse,
): Promise<ForwardResult> {
  await writeInbound(connection, message);
  return { ok: true };
}

async function forwardClientNotification(
  connection: ConnectionState,
  message: AnyMessage,
): Promise<ForwardResult> {
  await writeInbound(connection, message);
  return { ok: true };
}

function determineRoute(
  message: ClientRequestMessage,
  headers: Headers,
): RouteResult {
  const headerSessionId = headers.get(HEADER_SESSION_ID);

  if (headerSessionId) {
    return {
      ok: true,
      value: { session: headerSessionId },
    };
  }

  const paramsSessionId = sessionIdFromParams(message.params);

  if (paramsSessionId) {
    return {
      ok: true,
      value: { session: paramsSessionId },
    };
  }

  if (methodRequiresSessionHeader(message.method)) {
    return {
      ok: false,
      status: 400,
      message: "Missing Acp-Session-Id",
    };
  }

  return {
    ok: true,
    value: "connection",
  };
}

function sseResponse(subscription: OutboundSubscription): Response {
  return new Response(createSseBody(subscription), {
    status: 200,
    headers: {
      "Content-Type": EVENT_STREAM_MIME_TYPE,
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
}

function createSseBody(
  subscription: OutboundSubscription,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  let keepAliveTimer: ReturnType<typeof setInterval> | undefined;
  let reader: ReadableStreamDefaultReader<AnyMessage> | undefined;

  const clearKeepAlive = (): void => {
    if (keepAliveTimer) {
      clearInterval(keepAliveTimer);
      keepAliveTimer = undefined;
    }
  };

  const enqueueText = (
    controller: ReadableStreamDefaultController<Uint8Array>,
    text: string,
  ): boolean => {
    try {
      controller.enqueue(encoder.encode(text));
      return true;
    } catch {
      return false;
    }
  };

  return new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const message of subscription.replay) {
        if (!enqueueText(controller, serializeSseEvent(message))) {
          return;
        }
      }

      reader = subscription.stream.getReader();

      keepAliveTimer = setInterval(() => {
        if (!enqueueText(controller, serializeSseKeepAlive())) {
          clearKeepAlive();
        }
      }, 15_000);

      try {
        while (true) {
          const result = await reader.read();

          if (result.done) {
            return;
          }

          if (!enqueueText(controller, serializeSseEvent(result.value))) {
            return;
          }
        }
      } catch (error) {
        controller.error(error);
      } finally {
        clearKeepAlive();
        reader.releaseLock();

        try {
          controller.close();
        } catch {
          // Stream may already be cancelled by the consumer.
        }
      }
    },
    cancel() {
      clearKeepAlive();
      void reader?.cancel();
    },
  });
}

function jsonResponse(
  value: unknown,
  status: number,
  headers?: HeadersInit,
): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "Content-Type": JSON_MIME_TYPE,
      ...headers,
    },
  });
}

function textResponse(body: string, status: number): Response {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/plain",
    },
  });
}

function emptyResponse(status: number): Response {
  return new Response(null, { status });
}
