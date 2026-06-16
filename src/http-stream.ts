import { isJsonRpcMessage, isResponseMessage } from "./jsonrpc.js";
import {
  EVENT_STREAM_MIME_TYPE,
  HEADER_CONNECTION_ID,
  HEADER_SESSION_ID,
  JSON_MIME_TYPE,
  isInitializeRequest,
  messageIdKey,
  sessionIdFromMessageParams,
  sessionIdFromResponseResult,
} from "./protocol.js";
import { MemoryAcpCookieStore } from "./cookie-store.js";
import { parseSseStream } from "./sse.js";

import type { AcpCookieStore } from "./cookie-store.js";
import type { AnyMessage } from "./jsonrpc.js";
import type { Stream } from "./stream.js";

export interface HttpStreamOptions {
  /** Fetch implementation to use. Defaults to `globalThis.fetch`. */
  readonly fetch?: typeof globalThis.fetch;
  /** Headers to include on every HTTP/SSE request. */
  readonly headers?: Record<string, string>;
  /** Cookie handling policy for transport requests. Defaults to `include`. */
  readonly cookies?: "include" | "omit";
  /**
   * Caller-owned affinity cookie store to reuse across reconnects.
   *
   * If omitted, the SDK creates an ephemeral per-stream store and clears it
   * when the stream closes/errors.
   */
  readonly cookieStore?: AcpCookieStore;
}

export { MemoryAcpCookieStore } from "./cookie-store.js";
export type { AcpCookieStore } from "./cookie-store.js";

/**
 * Creates an ACP Stream over Streamable HTTP.
 *
 * Uses POST for client messages and SSE GET streams for server messages.
 * Cookies are included by default. Pass a caller-owned `AcpCookieStore` to
 * retain affinity cookies across fresh streams during reconnect.
 *
 * ACP v1 reconnect creates a new transport connection; callers should save the
 * ACP `sessionId`, create a new stream with the same auth headers/cookie store,
 * call `initialize`, verify `agentCapabilities.loadSession`, then call
 * `session/load`. Agents must authorize `session/load`, and ACP v1 does not
 * replay in-flight transport messages emitted while disconnected.
 */
export function createHttpStream(
  serverUrl: string,
  options: HttpStreamOptions = {},
): Stream {
  return new HttpStreamTransport(serverUrl, options).stream;
}

class HttpStreamTransport {
  readonly stream: Stream;

  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly headers: Record<string, string>;
  private readonly cookiePolicy: RequestCredentials;
  private readonly cookieStore: AcpCookieStore;
  private readonly ownsCookieStore: boolean;
  private readonly abortController = new AbortController();
  private readonly knownSessions = new Set<string>();
  private readonly pendingResponseSessions = new Map<string, string>();
  private readonly pendingSessionRequests = new Map<string, string>();

  private readableController:
    | ReadableStreamDefaultController<AnyMessage>
    | undefined;
  private connectionId: string | undefined;
  private isClosed = false;
  private writeChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly serverUrl: string,
    options: HttpStreamOptions,
  ) {
    this.fetchImpl = resolveFetch(options.fetch);
    this.headers = options.headers ?? {};
    this.cookiePolicy = options.cookies ?? "include";
    this.cookieStore = options.cookieStore ?? new MemoryAcpCookieStore();
    this.ownsCookieStore = options.cookieStore === undefined;

    this.stream = {
      readable: new ReadableStream<AnyMessage>({
        start: (controller) => {
          this.readableController = controller;
        },
        cancel: () => {
          void this.close();
        },
      }),
      writable: new WritableStream<AnyMessage>({
        write: (message) => {
          this.writeChain = this.writeChain.then(() =>
            this.writeMessage(message),
          );
          return this.writeChain;
        },
        close: () => this.close(),
        abort: () => this.close(),
      }),
    };
  }

  private async writeMessage(message: AnyMessage): Promise<void> {
    if (this.isClosed) {
      throw new Error("ACP HTTP stream is closed");
    }

    if (!this.connectionId) {
      await this.postInitialize(message);
      return;
    }

    await this.postConnectedMessage(message);
  }

  private async postInitialize(message: AnyMessage): Promise<void> {
    if (!isInitializeRequest(message)) {
      throw new Error("ACP HTTP stream first message must be initialize");
    }

    const response = await this.fetchRequest({
      method: "POST",
      headers: {
        "Content-Type": JSON_MIME_TYPE,
      },
      body: JSON.stringify(message),
    });

    if (!response.ok) {
      throw await httpError("ACP initialize failed", response);
    }

    const connectionId = response.headers.get(HEADER_CONNECTION_ID);
    if (!connectionId) {
      throw new Error("ACP initialize response missing Acp-Connection-Id");
    }

    const body: unknown = await response.json();
    if (!isJsonRpcMessage(body)) {
      throw new Error("ACP initialize response was not a JSON-RPC message");
    }

    this.connectionId = connectionId;
    this.openConnectionSse();
    this.enqueue(body);
  }

  private async postConnectedMessage(message: AnyMessage): Promise<void> {
    const connectionId = this.connectionId;
    if (!connectionId) {
      throw new Error("ACP HTTP stream is not initialized");
    }

    const sessionId = this.sessionIdForOutboundMessage(message);
    if (sessionId) {
      this.openSessionSse(sessionId);
    }

    const pendingSessionRequestKey =
      sessionId && "method" in message && "id" in message
        ? messageIdKey(message.id)
        : undefined;

    if (sessionId && pendingSessionRequestKey) {
      this.pendingSessionRequests.set(pendingSessionRequestKey, sessionId);
    }

    try {
      const response = await this.fetchRequest({
        method: "POST",
        headers: {
          "Content-Type": JSON_MIME_TYPE,
          [HEADER_CONNECTION_ID]: connectionId,
          ...(sessionId ? { [HEADER_SESSION_ID]: sessionId } : {}),
        },
        body: JSON.stringify(message),
      });

      if (!response.ok) {
        throw await httpError("ACP POST failed", response);
      }
    } catch (error) {
      if (pendingSessionRequestKey) {
        this.pendingSessionRequests.delete(pendingSessionRequestKey);
      }

      throw error;
    }
  }

  private sessionIdForOutboundMessage(message: AnyMessage): string | undefined {
    const paramsSessionId = sessionIdFromMessageParams(message);
    if (paramsSessionId) {
      return paramsSessionId;
    }

    if (!("id" in message) || "method" in message) {
      return undefined;
    }

    const key = messageIdKey(message.id);
    return key ? this.pendingResponseSessions.get(key) : undefined;
  }

  private openConnectionSse(): void {
    const connectionId = this.connectionId;
    if (!connectionId) {
      return;
    }

    void this.openSse({
      [HEADER_CONNECTION_ID]: connectionId,
    });
  }

  private openSessionSse(sessionId: string): void {
    if (this.knownSessions.has(sessionId)) {
      return;
    }

    const connectionId = this.connectionId;
    if (!connectionId) {
      return;
    }

    this.knownSessions.add(sessionId);

    void this.openSse({
      [HEADER_CONNECTION_ID]: connectionId,
      [HEADER_SESSION_ID]: sessionId,
    });
  }

  private async openSse(headers: Record<string, string>): Promise<void> {
    const streamSessionId = headers[HEADER_SESSION_ID];

    try {
      const response = await this.fetchRequest({
        method: "GET",
        headers: {
          Accept: EVENT_STREAM_MIME_TYPE,
          ...headers,
        },
        signal: this.abortController.signal,
      });

      if (!response.ok) {
        throw await httpError("ACP SSE connection failed", response);
      }

      if (!response.body) {
        throw new Error("ACP SSE response missing body");
      }

      for await (const message of parseSseStream(response.body)) {
        if (this.isClosed) {
          return;
        }

        const sessionId = sessionIdFromResponseResult(message);
        if (sessionId) {
          this.openSessionSse(sessionId);
        }

        this.trackServerRequestRoute(message, headers[HEADER_SESSION_ID]);
        this.trackInboundResponse(message);
        this.enqueue(message);
      }

      this.handleSseEof(streamSessionId);
    } catch (error) {
      if (this.isClosed || this.abortController.signal.aborted) {
        return;
      }

      this.errorReadable(error);
    }
  }

  private handleSseEof(streamSessionId: string | undefined): void {
    if (this.isClosed || this.abortController.signal.aborted) {
      return;
    }

    if (!streamSessionId) {
      this.errorReadable(new Error("ACP connection SSE stream closed"));
      return;
    }

    this.knownSessions.delete(streamSessionId);

    if (this.hasPendingSessionRequest(streamSessionId)) {
      this.errorReadable(new Error("ACP session SSE stream closed"));
    }
  }

  private trackServerRequestRoute(
    message: AnyMessage,
    streamSessionId: string | undefined,
  ): void {
    if (!streamSessionId || !("method" in message) || !("id" in message)) {
      return;
    }

    const key = messageIdKey(message.id);
    if (key) {
      this.pendingResponseSessions.set(key, streamSessionId);
    }
  }

  private trackInboundResponse(message: AnyMessage): void {
    if (!isResponseMessage(message)) {
      return;
    }

    const key = messageIdKey(message.id);
    if (key) {
      this.pendingSessionRequests.delete(key);
    }
  }

  private hasPendingSessionRequest(sessionId: string): boolean {
    for (const pendingSessionId of this.pendingSessionRequests.values()) {
      if (pendingSessionId === sessionId) {
        return true;
      }
    }

    return false;
  }

  private async fetchRequest(init: RequestInit): Promise<Response> {
    const response = await this.fetchImpl(this.serverUrl, {
      ...init,
      credentials: this.cookiePolicy,
      headers: this.createRequestHeaders(init.headers),
    });

    if (this.cookiePolicy === "include") {
      this.cookieStore.store(response.headers);
    }

    return response;
  }

  private createRequestHeaders(headers: HeadersInit | undefined): Headers {
    const requestHeaders = new Headers(this.headers);
    const transportHeaders = new Headers(headers);

    transportHeaders.forEach((value, key) => {
      requestHeaders.set(key, value);
    });

    if (this.cookiePolicy === "include") {
      this.cookieStore.apply(requestHeaders);
    }

    return requestHeaders;
  }

  private async close(): Promise<void> {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;

    const connectionId = this.connectionId;
    if (connectionId) {
      const response = await this.fetchRequest({
        method: "DELETE",
        headers: {
          [HEADER_CONNECTION_ID]: connectionId,
        },
      });

      if (!response.ok) {
        this.abortController.abort();
        this.clearOwnedCookieStore();
        this.closeReadable();
        throw await httpError("ACP DELETE failed", response);
      }
    }

    this.abortController.abort();
    this.clearOwnedCookieStore();
    this.closeReadable();
  }

  private clearOwnedCookieStore(): void {
    if (this.ownsCookieStore) {
      this.cookieStore.clear();
    }
  }

  private enqueue(message: AnyMessage): void {
    try {
      this.readableController?.enqueue(message);
    } catch (error) {
      this.errorReadable(error);
    }
  }

  private errorReadable(error: unknown): void {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;
    this.abortController.abort();
    this.clearOwnedCookieStore();

    try {
      this.readableController?.error(error);
    } catch {
      // The readable side may already be closed or cancelled.
    }
  }

  private closeReadable(): void {
    try {
      this.readableController?.close();
    } catch {
      // The readable side may already be closed, cancelled, or errored.
    }
  }
}

function resolveFetch(
  fetchImpl: typeof globalThis.fetch | undefined,
): typeof globalThis.fetch {
  if (fetchImpl) {
    return fetchImpl;
  }

  if (typeof globalThis.fetch === "function") {
    return (input, init) => globalThis.fetch(input, init);
  }

  throw new Error(
    "createHttpStream requires globalThis.fetch or options.fetch",
  );
}

async function httpError(prefix: string, response: Response): Promise<Error> {
  const text = await response.text().catch(() => "");

  if (text) {
    return new Error(
      `${prefix}: ${response.status} ${response.statusText}: ${text}`,
    );
  }

  return new Error(`${prefix}: ${response.status} ${response.statusText}`);
}
