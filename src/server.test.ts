import { describe, expect, it } from "vitest";
import { HEADER_CONNECTION_ID, JSON_MIME_TYPE } from "./protocol.js";
import { startTestServer } from "./test-support/test-http-server.js";
import { TestAgent } from "./test-support/test-agent.js";

import type { AgentSideConnection } from "./acp.js";

const initializeRequest = {
  jsonrpc: "2.0",
  id: 1,
  method: "initialize",
  params: {
    protocolVersion: 1,
    clientCapabilities: {},
  },
};

describe("AcpServer", () => {
  it("handles initialize over HTTP and returns a connection ID", async () => {
    const server = await startTestServer();

    try {
      const response = await postJson(server.url, initializeRequest);
      const body = await response.json();

      expect(response.status).toBe(200);
      expect(response.headers.get(HEADER_CONNECTION_ID)).toMatch(
        /^[0-9a-f-]{36}$/,
      );
      expect(body).toMatchObject({
        jsonrpc: "2.0",
        id: 1,
        result: {
          protocolVersion: 1,
          agentCapabilities: {
            loadSession: false,
          },
        },
      });
    } finally {
      await server.close();
    }
  });

  it.each(["GET", "PUT", "PATCH", "DELETE"])(
    "rejects %s requests in Phase 1",
    async (method) => {
      const server = await startTestServer();

      try {
        const response = await fetch(server.url, { method });

        expect(response.status).toBe(405);
      } finally {
        await server.close();
      }
    },
  );

  it("rejects POST without application/json Content-Type", async () => {
    const server = await startTestServer();

    try {
      const response = await fetch(server.url, {
        method: "POST",
        headers: {
          "Content-Type": "text/plain",
        },
        body: JSON.stringify(initializeRequest),
      });

      expect(response.status).toBe(415);
    } finally {
      await server.close();
    }
  });

  it("rejects invalid JSON", async () => {
    const server = await startTestServer();

    try {
      const response = await fetch(server.url, {
        method: "POST",
        headers: {
          "Content-Type": JSON_MIME_TYPE,
        },
        body: "{ nope",
      });

      expect(response.status).toBe(400);
    } finally {
      await server.close();
    }
  });

  it("rejects JSON-RPC batches", async () => {
    const server = await startTestServer();

    try {
      const response = await postJson(server.url, [initializeRequest]);

      expect(response.status).toBe(501);
    } finally {
      await server.close();
    }
  });

  it.each([
    null,
    "initialize",
    1,
    {},
    { jsonrpc: "1.0", method: "initialize" },
  ])("rejects invalid JSON-RPC messages", async (body) => {
    const server = await startTestServer();

    try {
      const response = await postJson(server.url, body);

      expect(response.status).toBe(400);
    } finally {
      await server.close();
    }
  });

  it("rejects non-initialize requests without a connection ID", async () => {
    const server = await startTestServer();

    try {
      const response = await postJson(server.url, {
        jsonrpc: "2.0",
        id: 2,
        method: "session/new",
        params: {
          cwd: "/tmp",
          mcpServers: [],
        },
      });

      expect(response.status).toBe(400);
    } finally {
      await server.close();
    }
  });

  it("rejects unknown connection IDs", async () => {
    const server = await startTestServer();

    try {
      const response = await postJson(
        server.url,
        {
          jsonrpc: "2.0",
          id: 2,
          method: "session/new",
          params: {
            cwd: "/tmp",
            mcpServers: [],
          },
        },
        {
          [HEADER_CONNECTION_ID]: globalThis.crypto.randomUUID(),
        },
      );

      expect(response.status).toBe(404);
    } finally {
      await server.close();
    }
  });

  it("rejects connected POSTs after initialize in Phase 1", async () => {
    const server = await startTestServer();

    try {
      const initializeResponse = await postJson(server.url, initializeRequest);
      const connectionId = initializeResponse.headers.get(HEADER_CONNECTION_ID);

      expect(connectionId).toBeTruthy();

      const response = await postJson(
        server.url,
        {
          jsonrpc: "2.0",
          id: 2,
          method: "session/new",
          params: {
            cwd: "/tmp",
            mcpServers: [],
          },
        },
        {
          [HEADER_CONNECTION_ID]: connectionId ?? "",
        },
      );

      expect(response.status).toBe(400);
    } finally {
      await server.close();
    }
  });

  it("returns an error response when agent creation fails", async () => {
    const server = await startTestServer(() => {
      throw new Error("agent factory failed");
    });

    try {
      const response = await postJson(server.url, initializeRequest);
      const body = await response.json();

      expect(response.status).toBe(500);
      expect(response.headers.get(HEADER_CONNECTION_ID)).toBeNull();
      expect(body).toMatchObject({
        jsonrpc: "2.0",
        id: 1,
        error: {
          code: -32603,
          message: "Initialize failed",
          data: "agent factory failed",
        },
      });
    } finally {
      await server.close();
    }
  });

  it("returns JSON-RPC initialize errors as the initialize response", async () => {
    class FailingInitializeAgent extends TestAgent {
      initialize() {
        return Promise.reject(new Error("initialize failed"));
      }
    }

    const server = await startTestServer(
      (conn: AgentSideConnection) => new FailingInitializeAgent(conn),
    );

    try {
      const response = await postJson(server.url, initializeRequest);
      const body = await response.json();

      expect(response.status).toBe(200);
      expect(response.headers.get(HEADER_CONNECTION_ID)).toMatch(
        /^[0-9a-f-]{36}$/,
      );
      expect(body).toMatchObject({
        jsonrpc: "2.0",
        id: 1,
        error: {
          code: -32603,
          message: "Internal error",
        },
      });
    } finally {
      await server.close();
    }
  });
});

function postJson(
  url: string,
  body: unknown,
  headers: Record<string, string> = {},
): Promise<Response> {
  return fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": JSON_MIME_TYPE,
      ...headers,
    },
    body: JSON.stringify(body),
  });
}
