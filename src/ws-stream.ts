import { isJsonRpcMessage } from "./jsonrpc.js";
import { onWebSocket, webSocketMessageToString } from "./ws-utils.js";
import type { WebSocketLike } from "./ws-utils.js";
import type { AnyMessage } from "./jsonrpc.js";
import type { Stream } from "./stream.js";

export interface WebSocketStreamOptions {
  /** WebSocket subprotocols to request. */
  readonly protocols?: string[];
  /**
   * Headers for WebSocket constructors that support them, such as Node `ws`.
   * Browser WebSocket constructors ignore custom headers.
   */
  readonly headers?: Record<string, string>;
  /** WebSocket constructor to use. Defaults to `globalThis.WebSocket`. */
  readonly WebSocket?: WebSocketConstructor;
}

/** Constructor shape used by `createWebSocketStream`. */
export interface WebSocketConstructor {
  new (
    url: string,
    protocols?: string | string[],
    options?: { headers?: Record<string, string> },
  ): WebSocketLike;
}

export type { WebSocketLike };

const SOCKET_OPEN = 1;

/**
 * Creates an ACP Stream over WebSocket.
 *
 * Sends and receives ACP JSON-RPC messages as WebSocket text frames. In Node,
 * pass a WebSocket constructor such as `ws.WebSocket` via `options.WebSocket`.
 */
export function createWebSocketStream(
  serverUrl: string,
  options: WebSocketStreamOptions = {},
): Stream {
  return new WebSocketStreamTransport(serverUrl, options).stream;
}

class WebSocketStreamTransport {
  readonly stream: Stream;

  private readonly socket: WebSocketLike;
  private readableController:
    | ReadableStreamDefaultController<AnyMessage>
    | undefined;
  private isClosed = false;
  private openPromise: Promise<void> | undefined;
  private resolveOpen: (() => void) | undefined;
  private rejectOpen: ((error: unknown) => void) | undefined;
  private readonly detachListeners: Array<() => void> = [];

  constructor(serverUrl: string, options: WebSocketStreamOptions) {
    const WebSocketCtor = resolveWebSocket(options.WebSocket);
    this.socket = new WebSocketCtor(serverUrl, options.protocols, {
      headers: options.headers,
    });

    this.openPromise = new Promise<void>((resolve, reject) => {
      this.resolveOpen = resolve;
      this.rejectOpen = reject;
    });

    this.detachListeners.push(
      onWebSocket(this.socket, "open", () => {
        this.resolveOpen?.();
        this.resolveOpen = undefined;
        this.rejectOpen = undefined;
        this.openPromise = undefined;
      }),
    );

    this.detachListeners.push(
      onWebSocket(this.socket, "message", (...args) => {
        this.handleSocketMessage(args);
      }),
    );

    this.detachListeners.push(
      onWebSocket(this.socket, "close", () => {
        this.closeReadable();
      }),
    );

    this.detachListeners.push(
      onWebSocket(this.socket, "error", (error) => {
        this.errorReadable(error);
      }),
    );

    this.stream = {
      readable: new ReadableStream<AnyMessage>({
        start: (controller) => {
          this.readableController = controller;
        },
        cancel: () => {
          this.close();
        },
      }),
      writable: new WritableStream<AnyMessage>({
        write: async (message) => {
          await this.sendMessage(message);
        },
        close: () => {
          this.close();
        },
        abort: () => {
          this.close();
        },
      }),
    };
  }

  private async sendMessage(message: AnyMessage): Promise<void> {
    if (this.isClosed) {
      throw new Error("ACP WebSocket stream is closed");
    }

    await this.waitForOpen();

    if (this.isClosed) {
      throw new Error("ACP WebSocket stream is closed");
    }

    this.socket.send(JSON.stringify(message));
  }

  private async waitForOpen(): Promise<void> {
    if (
      this.socket.readyState === undefined ||
      this.socket.readyState === SOCKET_OPEN
    ) {
      return;
    }

    await this.openPromise;
  }

  private handleSocketMessage(args: unknown[]): void {
    if (this.isClosed) {
      return;
    }

    const text = webSocketMessageToString(args);
    if (text === undefined) {
      return;
    }

    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch (error) {
      console.warn("Ignoring malformed ACP WebSocket JSON message:", error);
      return;
    }

    if (!isJsonRpcMessage(value)) {
      console.warn("Ignoring non-JSON-RPC ACP WebSocket message:", value);
      return;
    }

    this.readableController?.enqueue(value);
  }

  private close(): void {
    this.closeSocket();
    this.closeReadable();
  }

  private closeSocket(): void {
    try {
      this.socket.close();
    } catch (error) {
      console.warn("Failed to close ACP WebSocket:", error);
    }
  }

  private closeReadable(): void {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;

    for (const detach of this.detachListeners.splice(0)) {
      detach();
    }

    this.rejectOpen?.(new Error("ACP WebSocket stream closed before open"));
    this.rejectOpen = undefined;
    this.resolveOpen = undefined;
    this.openPromise = undefined;

    try {
      this.readableController?.close();
    } catch {
      // Stream may already be closed/cancelled.
    }
  }

  private errorReadable(error: unknown): void {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;

    for (const detach of this.detachListeners.splice(0)) {
      detach();
    }

    this.rejectOpen?.(error);
    this.rejectOpen = undefined;
    this.resolveOpen = undefined;
    this.openPromise = undefined;

    this.readableController?.error(error);
  }
}

function resolveWebSocket(
  WebSocketCtor: WebSocketConstructor | undefined,
): WebSocketConstructor {
  if (WebSocketCtor) {
    return WebSocketCtor;
  }

  if (typeof globalThis.WebSocket === "function") {
    return globalThis.WebSocket as unknown as WebSocketConstructor;
  }

  throw new Error(
    "createWebSocketStream requires globalThis.WebSocket or options.WebSocket",
  );
}
