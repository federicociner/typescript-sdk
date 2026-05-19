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
import { onWebSocket, webSocketMessageToString } from "./ws-utils.js";
import type { Agent, AgentSideConnection } from "./acp.js";
import type {
  ConnectionRegistry,
  ConnectionState,
  ResponseRoute,
} from "./connection.js";
import type { AnyMessage, AnyRequest } from "./jsonrpc.js";
import type { WebSocketLike } from "./ws-utils.js";

/** WebSocket shape accepted by prepared ACP WebSocket upgrades. */
export type WebSocketServerSocket = WebSocketLike;

type ForwardResult =
  | {
      ok: true;
    }
  | {
      ok: false;
      message: string;
    };

export interface WebSocketConnectionOptions {
  readonly registry: ConnectionRegistry;
  readonly createAgent: (conn: AgentSideConnection) => Agent;
  readonly connection?: ConnectionState;
}

export function handleWebSocketConnection(
  socket: WebSocketLike,
  options: WebSocketConnectionOptions,
): void {
  const session = new WebSocketServerSession(socket, options);
  session.start();
}

class WebSocketServerSession {
  private connection: ConnectionState | undefined;
  private preparedConnection: ConnectionState | undefined;
  private outboundReader: ReadableStreamDefaultReader<AnyMessage> | undefined;
  private inboundWriteChain: Promise<void> = Promise.resolve();
  private isClosed = false;
  private readonly detachListeners: Array<() => void> = [];

  constructor(
    private readonly socket: WebSocketLike,
    private readonly options: WebSocketConnectionOptions,
  ) {
    this.preparedConnection = options.connection;
  }

  start(): void {
    this.detachListeners.push(
      onWebSocket(this.socket, "message", (...args) => {
        void this.handleSocketMessage(args);
      }),
    );

    this.detachListeners.push(
      onWebSocket(this.socket, "close", () => {
        void this.closeSession();
      }),
    );

    this.detachListeners.push(
      onWebSocket(this.socket, "error", () => {
        void this.shutdown(1011, "WebSocket error");
      }),
    );
  }

  private async handleSocketMessage(args: unknown[]): Promise<void> {
    if (this.isClosed) {
      return;
    }

    const text = webSocketMessageToString(args);
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

    const connection =
      this.preparedConnection ??
      this.options.registry.createConnection(this.options.createAgent);
    this.preparedConnection = connection;

    try {
      await writeInbound(connection, message);

      const initialResponse = await connection.recvInitial(message.id);

      if (this.isClosed) {
        this.options.registry.remove(connection.connectionId);
        return;
      }

      this.preparedConnection = undefined;
      this.connection = connection;
      connection.startRouter();

      this.send(initialResponse);
      this.startOutboundPump(connection);
    } catch (error) {
      this.preparedConnection = undefined;
      this.options.registry.remove(connection.connectionId);

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

      await this.writeInbound(message);
      return { ok: true };
    }

    if (isResponseMessage(message)) {
      await this.writeInbound(message);
      return { ok: true };
    }

    await this.writeInbound(message);
    return { ok: true };
  }

  private async writeInbound(message: AnyMessage): Promise<void> {
    const connection = this.connection;

    if (!connection) {
      throw new Error("ACP WebSocket connection is not initialized");
    }

    const write = this.inboundWriteChain.then(() =>
      writeInbound(connection, message),
    );
    this.inboundWriteChain = write.catch(() => undefined);
    await write;
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

    if (this.preparedConnection) {
      this.options.registry.remove(this.preparedConnection.connectionId);
      this.preparedConnection = undefined;
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
