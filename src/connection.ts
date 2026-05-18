import { AgentSideConnection } from "./acp.js";
import { messageIdKey, sessionIdFromParams } from "./protocol.js";

import type { Agent } from "./acp.js";
import type { AnyMessage, AnyResponse } from "./jsonrpc.js";
import type { Stream } from "./stream.js";

export type ResponseRoute = "connection" | { readonly session: string };

export interface OutboundSubscription {
  readonly replay: readonly AnyMessage[];
  readonly stream: ReadableStream<AnyMessage>;
}

export class OutboundStream {
  private readonly subscribers = new Set<OutboundSubscriber>();
  private replayBuffer: AnyMessage[] = [];
  private hasSubscriber = false;
  private isClosed = false;

  constructor(private readonly capacity = 1024) {}

  push(message: AnyMessage): void {
    if (this.isClosed) {
      return;
    }

    if (!this.hasSubscriber) {
      this.replayBuffer.push(message);

      if (this.replayBuffer.length > this.capacity) {
        this.replayBuffer.shift();
      }

      return;
    }

    for (const subscriber of this.subscribers) {
      subscriber.push(message);
    }
  }

  subscribe(): OutboundSubscription {
    const replay = this.hasSubscriber ? [] : [...this.replayBuffer];
    this.replayBuffer = [];
    this.hasSubscriber = true;

    const subscriber = new OutboundSubscriber(this.capacity, (item) => {
      this.subscribers.delete(item);
    });

    this.subscribers.add(subscriber);

    if (this.isClosed) {
      subscriber.close();
    }

    return {
      replay,
      stream: subscriber.stream,
    };
  }

  close(): void {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;
    this.replayBuffer = [];

    for (const subscriber of this.subscribers) {
      subscriber.close();
    }

    this.subscribers.clear();
  }
}

export class ConnectionState {
  readonly connectionId: string;
  readonly inboundTx: WritableStream<AnyMessage>;
  readonly outboundRx: ReadableStream<AnyMessage>;
  readonly agentConnection: AgentSideConnection;
  readonly connectionStream = new OutboundStream();
  readonly allOutbound = new OutboundStream();
  readonly sessionStreams = new Map<string, OutboundStream>();
  readonly pendingRoutes = new Map<string, ResponseRoute>();

  private hasStartedRouter = false;
  private outboundReader: ReadableStreamDefaultReader<AnyMessage> | undefined;

  constructor(agentFactory: (conn: AgentSideConnection) => Agent) {
    this.connectionId = globalThis.crypto.randomUUID();
    const inbound = new TransformStream<AnyMessage, AnyMessage>();
    const outbound = new TransformStream<AnyMessage, AnyMessage>();

    this.inboundTx = inbound.writable;
    this.outboundRx = outbound.readable;

    const stream: Stream = {
      readable: inbound.readable,
      writable: outbound.writable,
    };

    this.agentConnection = new AgentSideConnection(agentFactory, stream);
  }

  async recvInitial(initializeId: string | number): Promise<AnyResponse> {
    const reader = this.outboundRx.getReader();

    try {
      const result = await reader.read();

      if (
        result.done ||
        !result.value ||
        !isMatchingResponse(result.value, initializeId)
      ) {
        await this.shutdown();
        throw new Error("Expected initialize response from agent");
      }

      return result.value;
    } finally {
      reader.releaseLock();
    }
  }

  startRouter(): void {
    if (this.hasStartedRouter) {
      return;
    }

    this.hasStartedRouter = true;
    void this.runRouter();
  }

  ensureSession(sessionId: string): OutboundStream {
    const existing = this.sessionStreams.get(sessionId);
    if (existing) {
      return existing;
    }

    const stream = new OutboundStream();
    this.sessionStreams.set(sessionId, stream);

    return stream;
  }

  async shutdown(): Promise<void> {
    this.connectionStream.close();
    this.allOutbound.close();

    for (const stream of this.sessionStreams.values()) {
      stream.close();
    }

    this.sessionStreams.clear();
    this.pendingRoutes.clear();

    await Promise.allSettled([
      this.inboundTx.close(),
      this.outboundReader?.cancel() ?? this.outboundRx.cancel(),
    ]);
  }

  private async runRouter(): Promise<void> {
    const reader = this.outboundRx.getReader();
    this.outboundReader = reader;

    try {
      while (true) {
        const result = await reader.read();

        if (result.done) {
          return;
        }

        this.routeOutbound(result.value);
      }
    } catch (error) {
      console.error("ACP connection router stopped unexpectedly:", error);
    } finally {
      if (this.outboundReader === reader) {
        this.outboundReader = undefined;
      }

      reader.releaseLock();
      this.connectionStream.close();
      this.allOutbound.close();

      for (const stream of this.sessionStreams.values()) {
        stream.close();
      }
    }
  }

  private routeOutbound(message: AnyMessage): void {
    this.allOutbound.push(message);

    if (isResponse(message)) {
      const key = messageIdKey(message.id);
      const route = key ? this.pendingRoutes.get(key) : undefined;
      const sessionId = sessionIdFromResult(
        "result" in message ? message.result : undefined,
      );

      if (sessionId) {
        this.ensureSession(sessionId);
      }

      if (key) {
        this.pendingRoutes.delete(key);
      }

      this.pushToRoute(route ?? "connection", message);
      return;
    }

    if ("method" in message) {
      const sessionId = sessionIdFromParams(message.params);
      if (sessionId) {
        this.ensureSession(sessionId).push(message);
        return;
      }
    }

    this.connectionStream.push(message);
  }

  private pushToRoute(route: ResponseRoute, message: AnyMessage): void {
    if (route === "connection") {
      this.connectionStream.push(message);
      return;
    }

    this.ensureSession(route.session).push(message);
  }
}

export class ConnectionRegistry {
  private readonly connections = new Map<string, ConnectionState>();

  createConnection(
    agentFactory: (conn: AgentSideConnection) => Agent,
  ): ConnectionState {
    const connection = new ConnectionState(agentFactory);
    this.connections.set(connection.connectionId, connection);
    return connection;
  }

  get(connectionId: string): ConnectionState | undefined {
    return this.connections.get(connectionId);
  }

  remove(connectionId: string): ConnectionState | undefined {
    const connection = this.get(connectionId);

    if (!connection) {
      return undefined;
    }

    this.connections.delete(connectionId);
    void connection.shutdown();
    return connection;
  }

  closeAll(): void {
    for (const connection of this.connections.values()) {
      void connection.shutdown();
    }

    this.connections.clear();
  }
}

class OutboundSubscriber {
  readonly stream: ReadableStream<AnyMessage>;

  private controller: ReadableStreamDefaultController<AnyMessage> | undefined;
  private queue: AnyMessage[] = [];
  private isClosed = false;
  private hasWarnedAboutOverflow = false;

  constructor(
    private readonly capacity: number,
    private readonly onCancel: (subscriber: OutboundSubscriber) => void,
  ) {
    this.stream = new ReadableStream<AnyMessage>({
      start: (controller) => {
        this.controller = controller;
        this.flush();
      },
      pull: () => {
        this.flush();
      },
      cancel: () => {
        this.cancel();
      },
    });
  }

  push(message: AnyMessage): void {
    if (this.isClosed) {
      return;
    }

    this.queue.push(message);

    if (this.queue.length > this.capacity) {
      this.queue.shift();

      if (!this.hasWarnedAboutOverflow) {
        console.warn("ACP outbound subscriber lagged; dropping oldest message");
        this.hasWarnedAboutOverflow = true;
      }
    }

    this.flush();
  }

  close(): void {
    if (this.isClosed) {
      return;
    }

    this.isClosed = true;
    this.queue = [];
    this.controller?.close();
  }

  private cancel(): void {
    this.isClosed = true;
    this.queue = [];
    this.onCancel(this);
  }

  private flush(): void {
    if (!this.controller) {
      return;
    }

    while (
      this.queue.length > 0 &&
      this.controller.desiredSize !== null &&
      this.controller.desiredSize > 0
    ) {
      const message = this.queue.shift();

      if (!message) {
        return;
      }

      this.controller.enqueue(message);
    }

    if (this.queue.length === 0) {
      this.hasWarnedAboutOverflow = false;
    }
  }
}

function isMatchingResponse(
  msg: AnyMessage,
  id: string | number,
): msg is AnyResponse {
  return "id" in msg && !("method" in msg) && msg.id === id;
}

function isResponse(msg: AnyMessage): msg is AnyResponse {
  return "id" in msg && !("method" in msg);
}

function sessionIdFromResult(result: unknown): string | undefined {
  if (!isRecord(result)) {
    return undefined;
  }

  const sessionId = result["sessionId"];
  return typeof sessionId === "string" ? sessionId : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
