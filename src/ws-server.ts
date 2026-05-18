import {
  isJsonRpcMessage,
  isRequestMessage,
  isResponseMessage,
} from "./jsonrpc.js";
import {
  isInitializeRequest,
  messageIdKey,
  sessionIdFromParams,
} from "./protocol.js";

import type { Agent, AgentSideConnection } from "./acp.js";
import type {
  ConnectionRegistry,
  ConnectionState,
  ResponseRoute,
} from "./connection.js";
import type { AnyMessage, AnyRequest } from "./jsonrpc.js";

type ForwardResult =
  | {
      ok: true;
    }
  | {
      ok: false;
      message: string;
    };

export interface WebSocketServerSocket {
  readonly readyState?: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener?(type: string, listener: (event: unknown) => void): void;
  removeEventListener?(type: string, listener: (event: unknown) => void): void;
  on?(type: string, listener: (...args: unknown[]) => void): unknown;
  off?(type: string, listener: (...args: unknown[]) => void): unknown;
  removeListener?(
    type: string,
    listener: (...args: unknown[]) => void,
  ): unknown;
}

export interface WebSocketConnectionOptions {
  readonly registry: ConnectionRegistry;
  readonly createAgent: (conn: AgentSideConnection) => Agent;
}

export function handleWebSocketConnection(
  socket: WebSocketServerSocket,
  options: WebSocketConnectionOptions,
): void {
  const session = new WebSocketServerSession(socket, options);
  session.start();
}

class WebSocketServerSession {
  private connection: ConnectionState | undefined;
  private outboundReader: ReadableStreamDefaultReader<AnyMessage> | undefined;
  private isClosed = false;
  private readonly detachListeners: Array<() => void> = [];

  constructor(
    private readonly socket: WebSocketServerSocket,
    private readonly options: WebSocketConnectionOptions,
  ) {}

  start(): void {
    this.detachListeners.push(
      onSocket(this.socket, "message", (...args) => {
        void this.handleSocketMessage(args);
      }),
    );

    this.detachListeners.push(
      onSocket(this.socket, "close", () => {
        void this.closeSession();
      }),
    );

    this.detachListeners.push(
      onSocket(this.socket, "error", () => {
        void this.shutdown(1011, "WebSocket error");
      }),
    );
  }

  private async handleSocketMessage(args: unknown[]): Promise<void> {
    if (this.isClosed) {
      return;
    }

    const text = socketMessageToString(args);
    if (text === undefined) {
      console.warn("Ignoring non-text ACP WebSocket frame");
      return;
    }

    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch (error) {
      console.warn("Ignoring malformed ACP WebSocket JSON message:", error);
      await this.shutdownIfUninitialized(1007, "Malformed JSON");

      return;
    }

    if (Array.isArray(value)) {
      console.warn("Ignoring ACP WebSocket JSON-RPC batch message");
      await this.shutdownIfUninitialized(
        1002,
        "JSON-RPC batch messages are not supported",
      );
      return;
    }

    if (!isJsonRpcMessage(value)) {
      console.warn("Ignoring non-JSON-RPC ACP WebSocket message:", value);
      await this.shutdownIfUninitialized(1002, "Invalid JSON-RPC message");
      return;
    }

    if (!this.connection) {
      await this.handleInitialize(value);
      return;
    }

    const forwarded = await this.forwardMessage(value);
    if (!forwarded.ok) {
      console.warn("Ignoring ACP WebSocket message:", forwarded.message);
    }
  }

  private async handleInitialize(message: AnyMessage): Promise<void> {
    if (!isInitializeRequest(message)) {
      console.warn("First ACP WebSocket message must be initialize");
      await this.shutdown(1002, "First message must be initialize");
      return;
    }

    if (!("id" in message) || message.id === null) {
      console.warn("ACP WebSocket initialize request must include an ID");
      await this.shutdown(1002, "Initialize request must include an ID");
      return;
    }

    let connection: ConnectionState | undefined;

    try {
      connection = this.options.registry.createConnection(
        this.options.createAgent,
      );

      await writeInbound(connection, message);

      const initialResponse = await connection.recvInitial(message.id);

      this.connection = connection;
      connection.startRouter();

      this.send(initialResponse);
      this.startOutboundPump(connection);
    } catch (error) {
      if (connection) {
        this.options.registry.remove(connection.connectionId);
      }

      this.send({
        jsonrpc: "2.0",
        id: message.id,
        error: {
          code: -32603,
          message: "Initialize failed",
          data: error instanceof Error ? error.message : undefined,
        },
      });

      await this.shutdown(1011, "Initialize failed");
    }
  }

  private async forwardMessage(message: AnyMessage): Promise<ForwardResult> {
    const connection = this.connection;

    if (!connection) {
      return {
        ok: false,
        message: "ACP WebSocket connection is not initialized",
      };
    }

    if (isRequestMessage(message)) {
      const route = determineWebSocketRoute(message);

      if (route !== "connection") {
        connection.ensureSession(route.session);
      }

      const key = messageIdKey(message.id);

      if (key) {
        connection.pendingRoutes.set(key, route);
      }

      await writeInbound(connection, message);
      return { ok: true };
    }

    if (isResponseMessage(message)) {
      await writeInbound(connection, message);
      return { ok: true };
    }

    await writeInbound(connection, message);
    return { ok: true };
  }

  private startOutboundPump(connection: ConnectionState): void {
    const subscription = connection.allOutbound.subscribe();
    const reader = subscription.stream.getReader();
    this.outboundReader = reader;

    void (async () => {
      try {
        for (const message of subscription.replay) {
          if (!this.send(message)) {
            return;
          }
        }

        while (!this.isClosed) {
          const result = await reader.read();

          if (result.done) {
            return;
          }

          if (!this.send(result.value)) {
            return;
          }
        }
      } catch (error) {
        if (!this.isClosed) {
          console.error("ACP WebSocket outbound pump failed:", error);
        }
      } finally {
        if (this.outboundReader === reader) {
          this.outboundReader = undefined;
        }

        reader.releaseLock();

        if (!this.isClosed) {
          void this.shutdown();
        }
      }
    })();
  }

  private send(message: AnyMessage): boolean {
    if (this.isClosed) {
      return false;
    }

    try {
      this.socket.send(JSON.stringify(message));
      return true;
    } catch (error) {
      console.warn("Failed to send ACP WebSocket message:", error);
      void this.shutdown(1011, "Failed to send message");
      return false;
    }
  }

  private async shutdownIfUninitialized(
    code?: number,
    reason?: string,
  ): Promise<void> {
    if (this.connection) {
      return;
    }

    await this.shutdown(code, reason);
  }

  private async shutdown(code?: number, reason?: string): Promise<void> {
    this.closeSocket(code, reason);
    await this.closeSession();
  }

  private closeSocket(code?: number, reason?: string): void {
    try {
      this.socket.close(code, reason);
    } catch (error) {
      console.warn("Failed to close ACP WebSocket:", error);
    }
  }

  private async closeSession(): Promise<void> {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;

    for (const detach of this.detachListeners.splice(0)) {
      detach();
    }

    const outboundReader = this.outboundReader;
    this.outboundReader = undefined;

    if (outboundReader) {
      await outboundReader.cancel();
    }

    if (this.connection) {
      this.options.registry.remove(this.connection.connectionId);
      this.connection = undefined;
    }
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

function determineWebSocketRoute(message: AnyRequest): ResponseRoute {
  const sessionId = sessionIdFromParams(message.params);

  if (sessionId) {
    return {
      session: sessionId,
    };
  }

  return "connection";
}

function onSocket(
  socket: WebSocketServerSocket,
  type: string,
  listener: (...args: unknown[]) => void,
): () => void {
  if (socket.addEventListener) {
    const eventListener = (event: unknown) => listener(event);
    socket.addEventListener(type, eventListener);

    return () => {
      socket.removeEventListener?.(type, eventListener);
    };
  }

  if (socket.on) {
    socket.on(type, listener);

    return () => {
      if (socket.off) {
        socket.off(type, listener);
        return;
      }

      socket.removeListener?.(type, listener);
    };
  }

  throw new Error("WebSocket object does not support event listeners");
}

function socketMessageToString(args: unknown[]): string | undefined {
  const data = extractMessageData(args);

  if (typeof data === "string") {
    return data;
  }

  if (data instanceof ArrayBuffer || ArrayBuffer.isView(data)) {
    return new TextDecoder().decode(data);
  }

  if (Array.isArray(data) && data.every(ArrayBuffer.isView)) {
    return decodeArrayBufferViews(data);
  }

  return undefined;
}

function extractMessageData(args: unknown[]): unknown {
  const [first] = args;

  if (isMessageEventLike(first)) {
    return first.data;
  }

  return first;
}

function isMessageEventLike(value: unknown): value is { data: unknown } {
  return typeof value === "object" && value !== null && "data" in value;
}

function decodeArrayBufferViews(views: ArrayBufferView[]): string {
  const totalLength = views.reduce((sum, view) => sum + view.byteLength, 0);
  const combined = new Uint8Array(totalLength);
  let offset = 0;

  for (const view of views) {
    combined.set(
      new Uint8Array(view.buffer, view.byteOffset, view.byteLength),
      offset,
    );
    offset += view.byteLength;
  }

  return new TextDecoder().decode(combined);
}
