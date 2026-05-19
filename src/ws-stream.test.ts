import { describe, expect, it } from "vitest";
import { WebSocket } from "ws";

import { ClientSideConnection, PROTOCOL_VERSION } from "./acp.js";
import { createWebSocketStream } from "./ws-stream.js";
import { TestAgent } from "./test-support/test-agent.js";
import { startTestServer } from "./test-support/test-http-server.js";

import type {
  AgentSideConnection,
  Client,
  RequestPermissionRequest,
  RequestPermissionResponse,
  SessionNotification,
} from "./acp.js";
import type { AnyMessage } from "./jsonrpc.js";
import type { Stream } from "./stream.js";
import type { WebSocketConstructor } from "./ws-stream.js";

const nodeWebSocket = WebSocket as unknown as WebSocketConstructor;

const initializeRequest = {
  jsonrpc: "2.0",
  id: 0,
  method: "initialize",
  params: {
    protocolVersion: PROTOCOL_VERSION,
    clientCapabilities: {},
  },
} satisfies AnyMessage;

const initializeResponse = {
  jsonrpc: "2.0",
  id: 0,
  result: {
    protocolVersion: PROTOCOL_VERSION,
    agentCapabilities: {
      loadSession: false,
    },
  },
} satisfies AnyMessage;

describe("createWebSocketStream", () => {
  it("uses the custom WebSocket constructor and queues writes until the socket opens", async () => {
    const instances: FakeWebSocket[] = [];
    const stream = createWebSocketStream("ws://agent.example/acp", {
      WebSocket: createFakeWebSocketConstructor(instances),
      protocols: ["acp"],
      headers: { Authorization: "Bearer token" },
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();

    try {
      const socket = fakeSocketAt(instances, 0);
      expect(socket.url).toBe("ws://agent.example/acp");
      expect(socket.protocols).toEqual(["acp"]);
      expect(socket.options).toEqual({
        headers: { Authorization: "Bearer token" },
      });

      const write = writer.write(initializeRequest);
      await Promise.resolve();
      expect(socket.sent).toEqual([]);

      socket.open();
      await write;
      expect(socket.sent).toEqual([JSON.stringify(initializeRequest)]);

      socket.receive(JSON.stringify(initializeResponse));
      expect(await readMessage(reader)).toEqual(initializeResponse);
    } finally {
      reader.releaseLock();
      await writer.close().catch(() => undefined);
      writer.releaseLock();
    }
  });

  it("ignores binary, malformed JSON, and non-JSON-RPC messages", async () => {
    const instances: FakeWebSocket[] = [];
    const stream = createWebSocketStream("ws://agent.example/acp", {
      WebSocket: createFakeWebSocketConstructor(instances),
    });
    const reader = stream.readable.getReader();

    try {
      const socket = fakeSocketAt(instances, 0);
      socket.open();
      socket.receive(new Uint8Array([1, 2, 3]), true);
      socket.receive("not json");
      socket.receive(JSON.stringify({ hello: "world" }));
      socket.receive(JSON.stringify(initializeResponse));

      expect(await readMessage(reader)).toEqual(initializeResponse);
    } finally {
      reader.releaseLock();
      await closeStream(stream);
    }
  });

  it("closes the readable stream when the socket closes", async () => {
    const instances: FakeWebSocket[] = [];
    const stream = createWebSocketStream("ws://agent.example/acp", {
      WebSocket: createFakeWebSocketConstructor(instances),
    });
    const reader = stream.readable.getReader();

    try {
      const socket = fakeSocketAt(instances, 0);
      socket.open();
      socket.close();

      expect(await reader.read()).toEqual({ done: true, value: undefined });
    } finally {
      reader.releaseLock();
    }
  });

  it("runs initialize, newSession, and prompt through ClientSideConnection", async () => {
    const updates: SessionNotification[] = [];
    const server = await startTestServer(
      (conn: AgentSideConnection) => new TestAgent(conn, { chunkCount: 2 }),
    );
    const stream = createWebSocketStream(server.wsUrl, {
      WebSocket: nodeWebSocket,
    });
    const conn = new ClientSideConnection(
      () => createTestClient({ updates }),
      stream,
    );

    try {
      expect(
        await conn.initialize({
          protocolVersion: PROTOCOL_VERSION,
          clientCapabilities: {},
        }),
      ).toMatchObject({
        protocolVersion: PROTOCOL_VERSION,
        agentCapabilities: { loadSession: false },
      });

      const session = await conn.newSession({ cwd: "/tmp", mcpServers: [] });
      expect(session.sessionId).toMatch(/^[0-9a-f-]{36}$/);

      await expect(
        conn.prompt({
          sessionId: session.sessionId,
          prompt: [{ type: "text", text: "Hello" }],
        }),
      ).resolves.toEqual({ stopReason: "end_turn" });
      expect(updates).toHaveLength(2);
      expect(updates).toMatchObject([
        {
          sessionId: session.sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { text: "chunk-1" },
          },
        },
        {
          sessionId: session.sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { text: "chunk-2" },
          },
        },
      ]);
    } finally {
      await closeStream(stream);
      await server.close();
    }
  });

  it("round-trips permission requests through ClientSideConnection", async () => {
    const updates: SessionNotification[] = [];
    const permissionRequests: RequestPermissionRequest[] = [];
    const server = await startTestServer(
      (conn: AgentSideConnection) =>
        new TestAgent(conn, { enablePermission: true }),
    );
    const stream = createWebSocketStream(server.wsUrl, {
      WebSocket: nodeWebSocket,
    });
    const conn = new ClientSideConnection(
      () => createTestClient({ updates, permissionRequests }),
      stream,
    );

    try {
      await conn.initialize({
        protocolVersion: PROTOCOL_VERSION,
        clientCapabilities: {},
      });
      const session = await conn.newSession({ cwd: "/tmp", mcpServers: [] });

      await expect(
        conn.prompt({
          sessionId: session.sessionId,
          prompt: [{ type: "text", text: "Hello" }],
        }),
      ).resolves.toEqual({ stopReason: "end_turn" });

      expect(permissionRequests).toHaveLength(1);
      expect(permissionRequests[0]).toMatchObject({
        sessionId: session.sessionId,
        toolCall: {
          toolCallId: "permission-tool",
          title: "Permission tool",
        },
      });
      expect(updates).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            sessionId: session.sessionId,
            update: expect.objectContaining({
              sessionUpdate: "agent_message_chunk",
              content: expect.objectContaining({
                text: "permission-selected-allow",
              }),
            }),
          }),
        ]),
      );
    } finally {
      await closeStream(stream);
      await server.close();
    }
  });

  it("keeps multiple sessions isolated through the SDK client abstraction", async () => {
    const updates: SessionNotification[] = [];
    const server = await startTestServer();
    const stream = createWebSocketStream(server.wsUrl, {
      WebSocket: nodeWebSocket,
    });
    const conn = new ClientSideConnection(
      () => createTestClient({ updates }),
      stream,
    );

    try {
      await conn.initialize({
        protocolVersion: PROTOCOL_VERSION,
        clientCapabilities: {},
      });
      const firstSession = await conn.newSession({
        cwd: "/tmp",
        mcpServers: [],
      });
      const secondSession = await conn.newSession({
        cwd: "/tmp/other",
        mcpServers: [],
      });

      await Promise.all([
        conn.prompt({
          sessionId: firstSession.sessionId,
          prompt: [{ type: "text", text: "First" }],
        }),
        conn.prompt({
          sessionId: secondSession.sessionId,
          prompt: [{ type: "text", text: "Second" }],
        }),
      ]);

      expect(updates).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ sessionId: firstSession.sessionId }),
          expect.objectContaining({ sessionId: secondSession.sessionId }),
        ]),
      );
      expect(
        updates.filter((update) => update.sessionId === firstSession.sessionId),
      ).toHaveLength(1);
      expect(
        updates.filter(
          (update) => update.sessionId === secondSession.sessionId,
        ),
      ).toHaveLength(1);
    } finally {
      await closeStream(stream);
      await server.close();
    }
  });
});

interface TestClientState {
  readonly updates: SessionNotification[];
  readonly permissionRequests?: RequestPermissionRequest[];
}

function createTestClient(state: TestClientState): Client {
  return {
    requestPermission: (params): Promise<RequestPermissionResponse> => {
      state.permissionRequests?.push(params);
      return Promise.resolve({
        outcome: {
          outcome: "selected",
          optionId: "allow",
        },
      });
    },
    sessionUpdate: (params): Promise<void> => {
      state.updates.push(params);
      return Promise.resolve();
    },
  };
}

async function closeStream(stream: Stream): Promise<void> {
  await stream.writable.close().catch(() => undefined);
}

async function readMessage(
  reader: ReadableStreamDefaultReader<AnyMessage>,
): Promise<AnyMessage> {
  const result = await reader.read();
  if (result.done) {
    throw new Error("Expected a message");
  }

  return result.value;
}

function createFakeWebSocketConstructor(
  instances: FakeWebSocket[],
): WebSocketConstructor {
  return class extends FakeWebSocket {
    constructor(
      url: string,
      protocols?: string | string[],
      options?: { headers?: Record<string, string> },
    ) {
      super(url, protocols, options);
      instances.push(this);
    }
  };
}

function fakeSocketAt(
  instances: readonly FakeWebSocket[],
  index: number,
): FakeWebSocket {
  const socket = instances[index];

  if (!socket) {
    throw new Error(`Expected fake WebSocket at index ${index}`);
  }

  return socket;
}

class FakeWebSocket {
  readonly sent: string[] = [];
  readonly listeners = new Map<string, Set<(event: unknown) => void>>();
  readyState = 0;

  constructor(
    readonly url: string,
    readonly protocols?: string | string[],
    readonly options?: { headers?: Record<string, string> },
  ) {}

  send(data: string): void {
    if (this.readyState !== 1) {
      throw new Error("Fake WebSocket is not open");
    }

    this.sent.push(data);
  }

  close(): void {
    if (this.readyState === 3) {
      return;
    }

    this.readyState = 3;
    this.emit("close", {});
  }

  addEventListener(type: string, listener: (event: unknown) => void): void {
    let listeners = this.listeners.get(type);

    if (!listeners) {
      listeners = new Set();
      this.listeners.set(type, listeners);
    }

    listeners.add(listener);
  }

  removeEventListener(type: string, listener: (event: unknown) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  open(): void {
    this.readyState = 1;
    this.emit("open", {});
  }

  receive(data: unknown, isBinary = false): void {
    this.emit("message", { data, isBinary });
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}
