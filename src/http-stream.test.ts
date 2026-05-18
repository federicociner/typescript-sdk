import { describe, expect, it } from "vitest";
import { ClientSideConnection, PROTOCOL_VERSION } from "./acp.js";
import { createHttpStream } from "./http-stream.js";
import {
  EVENT_STREAM_MIME_TYPE,
  HEADER_CONNECTION_ID,
  HEADER_SESSION_ID,
  JSON_MIME_TYPE,
} from "./protocol.js";
import { serializeSseEvent } from "./sse.js";
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

const sessionNewResponse = {
  jsonrpc: "2.0",
  id: 1,
  result: {
    sessionId: "session-1",
  },
} satisfies AnyMessage;

const promptRequest = {
  jsonrpc: "2.0",
  id: 2,
  method: "session/prompt",
  params: {
    sessionId: "session-1",
    prompt: [{ type: "text", text: "Hello" }],
  },
} satisfies AnyMessage;

describe("createHttpStream", () => {
  it("posts initialize with custom headers, opens connection SSE, and emits the initialize response", async () => {
    const controlledFetch = createControlledFetch();
    const stream = createHttpStream("https://agent.example/acp", {
      fetch: controlledFetch.fetch,
      headers: {
        Authorization: "Bearer token",
        "X-Test-Header": "phase-5",
      },
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();

    try {
      await writer.write(initializeRequest);

      expect(await readMessage(reader)).toEqual(initializeResponse);
      expect(controlledFetch.requests).toHaveLength(2);

      const initializePost = requestAt(controlledFetch.requests, 0);
      expect(initializePost.url).toBe("https://agent.example/acp");
      expect(initializePost.method).toBe("POST");
      expect(initializePost.headers.get("Authorization")).toBe("Bearer token");
      expect(initializePost.headers.get("X-Test-Header")).toBe("phase-5");
      expect(initializePost.headers.get("Content-Type")).toBe(JSON_MIME_TYPE);
      expect(initializePost.headers.get(HEADER_CONNECTION_ID)).toBeNull();
      expect(JSON.parse(initializePost.body)).toEqual(initializeRequest);

      const connectionGet = requestAt(controlledFetch.requests, 1);
      expect(connectionGet.method).toBe("GET");
      expect(connectionGet.headers.get("Authorization")).toBe("Bearer token");
      expect(connectionGet.headers.get("Accept")).toBe(EVENT_STREAM_MIME_TYPE);
      expect(connectionGet.headers.get(HEADER_CONNECTION_ID)).toBe(
        "connection-1",
      );
      expect(connectionGet.headers.get(HEADER_SESSION_ID)).toBeNull();
    } finally {
      reader.releaseLock();
      writer.releaseLock();
      await stream.writable.close();
    }
  });

  it("opens session SSE after session creation and includes the session header on session-scoped POSTs", async () => {
    const controlledFetch = createControlledFetch();
    const stream = createHttpStream("https://agent.example/acp", {
      fetch: controlledFetch.fetch,
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();

    try {
      await writer.write(initializeRequest);
      await readMessage(reader);
      await controlledFetch.sendSse(0, sessionNewResponse);

      expect(await readMessage(reader)).toEqual(sessionNewResponse);
      expect(controlledFetch.requests).toHaveLength(3);

      const sessionGet = requestAt(controlledFetch.requests, 2);
      expect(sessionGet.method).toBe("GET");
      expect(sessionGet.headers.get(HEADER_CONNECTION_ID)).toBe("connection-1");
      expect(sessionGet.headers.get(HEADER_SESSION_ID)).toBe("session-1");

      await writer.write(promptRequest);

      const promptPost = requestAt(controlledFetch.requests, 3);
      expect(promptPost.method).toBe("POST");
      expect(promptPost.headers.get(HEADER_CONNECTION_ID)).toBe("connection-1");
      expect(promptPost.headers.get(HEADER_SESSION_ID)).toBe("session-1");
      expect(JSON.parse(promptPost.body)).toEqual(promptRequest);
    } finally {
      reader.releaseLock();
      writer.releaseLock();
      await stream.writable.close();
    }
  });

  it("sends DELETE and aborts SSE requests when closed", async () => {
    const controlledFetch = createControlledFetch();
    const stream = createHttpStream("https://agent.example/acp", {
      fetch: controlledFetch.fetch,
    });
    const writer = stream.writable.getWriter();
    const reader = stream.readable.getReader();

    try {
      await writer.write(initializeRequest);
      await readMessage(reader);
      await writer.close();

      const deleteRequest = requestAt(controlledFetch.requests, 2);
      expect(deleteRequest.method).toBe("DELETE");
      expect(deleteRequest.headers.get(HEADER_CONNECTION_ID)).toBe(
        "connection-1",
      );
      expect(sseAt(controlledFetch.sseRequests, 0).signal.aborted).toBe(true);
    } finally {
      reader.releaseLock();
      writer.releaseLock();
    }
  });

  it("runs initialize, newSession, and prompt through ClientSideConnection", async () => {
    const updates: SessionNotification[] = [];
    const server = await startTestServer(
      (conn: AgentSideConnection) => new TestAgent(conn, { chunkCount: 2 }),
    );
    const stream = createHttpStream(server.url);
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
    const stream = createHttpStream(server.url);
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
    const stream = createHttpStream(server.url);
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

interface RecordedRequest {
  readonly url: string;
  readonly method: string;
  readonly headers: Headers;
  readonly body: string;
}

interface RecordedSseRequest {
  readonly signal: AbortSignal;
  readonly writer: WritableStreamDefaultWriter<Uint8Array>;
}

interface ControlledFetch {
  readonly fetch: typeof globalThis.fetch;
  readonly requests: RecordedRequest[];
  readonly sseRequests: RecordedSseRequest[];
  readonly sendSse: (index: number, message: AnyMessage) => Promise<void>;
}

interface TestClientState {
  readonly updates: SessionNotification[];
  readonly permissionRequests?: RequestPermissionRequest[];
}

function createControlledFetch(): ControlledFetch {
  const requests: RecordedRequest[] = [];
  const sseRequests: RecordedSseRequest[] = [];
  const encoder = new TextEncoder();

  return {
    requests,
    sseRequests,
    fetch: async (input, init) => {
      const method = init?.method ?? "GET";
      const headers = new Headers(init?.headers);
      requests.push({
        url: String(input),
        method,
        headers,
        body: bodyToString(init?.body),
      });

      if (method === "POST" && !headers.has(HEADER_CONNECTION_ID)) {
        return jsonResponse(initializeResponse, 200, {
          [HEADER_CONNECTION_ID]: "connection-1",
        });
      }

      if (method === "POST" || method === "DELETE") {
        return new Response(null, { status: 202 });
      }

      if (method === "GET") {
        const stream = new TransformStream<Uint8Array, Uint8Array>();
        const writer = stream.writable.getWriter();
        const signal = init?.signal;

        if (signal) {
          signal.addEventListener("abort", () => {
            void writer.close();
          });
        }

        sseRequests.push({
          signal: signal ?? new AbortController().signal,
          writer,
        });

        return new Response(stream.readable, {
          status: 200,
          headers: { "Content-Type": EVENT_STREAM_MIME_TYPE },
        });
      }

      return new Response("Unexpected method", { status: 405 });
    },
    sendSse: async (index, message) => {
      await sseAt(sseRequests, index).writer.write(
        encoder.encode(serializeSseEvent(message)),
      );
    },
  };
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

async function closeStream(stream: {
  writable: WritableStream<AnyMessage>;
}): Promise<void> {
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

function requestAt(
  requests: readonly RecordedRequest[],
  index: number,
): RecordedRequest {
  const request = requests[index];
  if (!request) {
    throw new Error(`Expected request at index ${index}`);
  }

  return request;
}

function sseAt(
  requests: readonly RecordedSseRequest[],
  index: number,
): RecordedSseRequest {
  const request = requests[index];
  if (!request) {
    throw new Error(`Expected SSE request at index ${index}`);
  }

  return request;
}

function bodyToString(body: BodyInit | null | undefined): string {
  return typeof body === "string" ? body : "";
}

function jsonResponse(
  body: AnyMessage,
  status: number,
  headers: Record<string, string>,
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": JSON_MIME_TYPE,
      ...headers,
    },
  });
}
