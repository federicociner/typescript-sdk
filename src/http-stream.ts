import { isJsonRpcMessage } from "./jsonrpc.js";
import {
  EVENT_STREAM_MIME_TYPE,
  HEADER_CONNECTION_ID,
  HEADER_SESSION_ID,
  JSON_MIME_TYPE,
  isInitializeRequest,
  sessionIdFromMessageParams,
  sessionIdFromResponseResult,
} from "./protocol.js";
import { parseSseStream } from "./sse.js";

import type { AnyMessage } from "./jsonrpc.js";
import type { Stream } from "./stream.js";

export interface HttpStreamOptions {
  /** Fetch implementation to use. Defaults to `globalThis.fetch`. */
  readonly fetch?: typeof globalThis.fetch;
  /** Headers to include on every HTTP/SSE request. */
  readonly headers?: Record<string, string>;
  /** Cookie handling policy for transport requests. Defaults to `include`. */
  readonly cookies?: "include" | "omit";
}

/**
 * Creates an ACP Stream over Streamable HTTP.
 *
 * Uses POST for client messages and SSE GET streams for server messages.
 * Cookies are included by default for the lifetime of one stream.
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
  private readonly cookieJar = new ConnectionCookieJar();
  private readonly abortController = new AbortController();
  private readonly knownSessions = new Set<string>();

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

    const sessionId = sessionIdFromMessageParams(message);
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

        this.enqueue(message);
      }
    } catch (error) {
      if (this.isClosed || this.abortController.signal.aborted) {
        return;
      }

      this.errorReadable(error);
    }
  }

  private async fetchRequest(init: RequestInit): Promise<Response> {
    const response = await this.fetchImpl(this.serverUrl, {
      ...init,
      credentials: this.cookiePolicy,
      headers: this.createRequestHeaders(init.headers),
    });

    if (this.cookiePolicy === "include") {
      this.cookieJar.store(response.headers);
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
      this.cookieJar.apply(requestHeaders);
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
        this.cookieJar.clear();
        this.closeReadable();
        throw await httpError("ACP DELETE failed", response);
      }
    }

    this.abortController.abort();
    this.cookieJar.clear();
    this.closeReadable();
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
    this.cookieJar.clear();

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

class ConnectionCookieJar {
  private readonly cookies = new Map<string, string>();

  store(headers: Headers): void {
    for (const value of setCookieHeaders(headers)) {
      const cookie = parseSetCookie(value);
      if (!cookie) {
        continue;
      }

      this.cookies.set(cookie.name, cookie.value);
    }
  }

  apply(headers: Headers): void {
    const merged = mergeCookieHeaders(
      this.cookieHeader(),
      headers.get("Cookie"),
    );
    if (merged) {
      headers.set("Cookie", merged);
    }
  }

  clear(): void {
    this.cookies.clear();
  }

  private cookieHeader(): string | undefined {
    return this.cookies.size === 0
      ? undefined
      : Array.from(this.cookies)
          .map(([name, value]) => `${name}=${value}`)
          .join("; ");
  }
}

interface CookiePair {
  readonly name: string;
  readonly value: string;
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

function setCookieHeaders(headers: Headers): string[] {
  const getSetCookie = headers.getSetCookie;
  if (typeof getSetCookie === "function") {
    return getSetCookie.call(headers);
  }

  const setCookie = headers.get("Set-Cookie");
  return setCookie ? splitSetCookieHeader(setCookie) : [];
}

function splitSetCookieHeader(header: string): string[] {
  const result: string[] = [];
  let start = 0;
  let isInExpires = false;

  for (let index = 0; index < header.length; index += 1) {
    const char = header[index];

    if (char === "," && !isInExpires) {
      result.push(header.slice(start, index).trim());
      start = index + 1;
      continue;
    }

    if (header.slice(index, index + 8).toLowerCase() === "expires=") {
      isInExpires = true;
      index += 7;
      continue;
    }

    if (char === ";" && isInExpires) {
      isInExpires = false;
    }
  }

  result.push(header.slice(start).trim());
  return result.filter((value) => value.length > 0);
}

function parseSetCookie(header: string): CookiePair | undefined {
  const pair = header.split(";", 1)[0];
  const separator = pair.indexOf("=");

  if (separator <= 0) {
    return undefined;
  }

  return {
    name: pair.slice(0, separator).trim(),
    value: pair.slice(separator + 1).trim(),
  };
}

function mergeCookieHeaders(
  jarCookieHeader: string | undefined,
  callerCookieHeader: string | null,
): string | undefined {
  const cookies = new Map<string, string>();

  for (const cookie of parseCookieHeader(jarCookieHeader)) {
    cookies.set(cookie.name, cookie.value);
  }

  for (const cookie of parseCookieHeader(callerCookieHeader ?? undefined)) {
    cookies.set(cookie.name, cookie.value);
  }

  return cookies.size === 0
    ? undefined
    : Array.from(cookies)
        .map(([name, value]) => `${name}=${value}`)
        .join("; ");
}

function parseCookieHeader(header: string | undefined): CookiePair[] {
  if (!header) {
    return [];
  }

  return header
    .split(";")
    .map(parseCookiePair)
    .filter((cookie): cookie is CookiePair => cookie !== undefined);
}

function parseCookiePair(value: string): CookiePair | undefined {
  const separator = value.indexOf("=");

  if (separator <= 0) {
    return undefined;
  }

  return {
    name: value.slice(0, separator).trim(),
    value: value.slice(separator + 1).trim(),
  };
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
