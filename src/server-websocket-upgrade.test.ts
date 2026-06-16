import { describe, expect, it } from "vitest";

import { PROTOCOL_VERSION } from "./acp.js";
import { HEADER_CONNECTION_ID, JSON_MIME_TYPE } from "./protocol.js";
import { AcpServer } from "./server.js";
import { TestAgent } from "./test-support/test-agent.js";

import type { Agent, AgentSideConnection } from "./acp.js";
import type { AnyMessage } from "./jsonrpc.js";
import type { WebSocketServerSocket } from "./ws-server.js";

const initializeRequest = {
  jsonrpc: "2.0",
  id: 0,
  method: "initialize",
  params: {
    protocolVersion: PROTOCOL_VERSION,
    clientCapabilities: {},
  },
} satisfies AnyMessage;

const sessionNewRequest = {
  jsonrpc: "2.0",
  id: 1,
  method: "session/new",
  params: {
    cwd: "/tmp",
    mcpServers: [],
  },
} satisfies AnyMessage;

describe("AcpServer prepared WebSocket upgrades", () => {
  it("uses the default factory when no per-upgrade override is provided", async () => {
    const createdBy: string[] = [];
    const server = new AcpServer({
      createAgent: recordingFactory(createdBy, "default"),
    });
    const socket = new FakeServerSocket();

    try {
      server.prepareWebSocketUpgrade().accept(socket);
      socket.receive(JSON.stringify(initializeRequest));

      await expect(readSentMessage(socket)).resolves.toMatchObject({
        jsonrpc: "2.0",
        id: initializeRequest.id,
        result: {
          protocolVersion: PROTOCOL_VERSION,
        },
      });
      expect(createdBy).toEqual(["default"]);
    } finally {
      socket.close();
      await server.close();
    }
  });

  it("uses a per-upgrade factory override for that WebSocket connection", async () => {
    const createdBy: string[] = [];
    const server = new AcpServer({
      createAgent: recordingFactory(createdBy, "default"),
    });
    const socket = new FakeServerSocket();

    try {
      server
        .prepareWebSocketUpgrade({
          createAgent: recordingFactory(createdBy, "override"),
        })
        .accept(socket);
      socket.receive(JSON.stringify(initializeRequest));

      await readSentMessage(socket);
      expect(createdBy).toEqual(["override"]);
    } finally {
      socket.close();
      await server.close();
    }
  });

  it("does not leak WebSocket factory overrides to later prepared upgrades", async () => {
    const createdBy: string[] = [];
    const server = new AcpServer({
      createAgent: recordingFactory(createdBy, "default"),
    });
    const overrideSocket = new FakeServerSocket();
    const defaultSocket = new FakeServerSocket();

    try {
      server
        .prepareWebSocketUpgrade({
          createAgent: recordingFactory(createdBy, "override"),
        })
        .accept(overrideSocket);
      server.prepareWebSocketUpgrade().accept(defaultSocket);

      overrideSocket.receive(JSON.stringify(initializeRequest));
      defaultSocket.receive(JSON.stringify({ ...initializeRequest, id: 1 }));

      await Promise.all([
        readSentMessage(overrideSocket),
        readSentMessage(defaultSocket),
      ]);
      expect(createdBy).toEqual(["override", "default"]);
    } finally {
      overrideSocket.close();
      defaultSocket.close();
      await server.close();
    }
  });

  it("keeps concurrent WebSocket factory overrides isolated", async () => {
    const createdBy: string[] = [];
    const server = new AcpServer({
      createAgent: recordingFactory(createdBy, "default"),
    });
    const firstSocket = new FakeServerSocket();
    const secondSocket = new FakeServerSocket();

    try {
      const first = server.prepareWebSocketUpgrade({
        createAgent: recordingFactory(createdBy, "first"),
      });
      const second = server.prepareWebSocketUpgrade({
        createAgent: recordingFactory(createdBy, "second"),
      });

      second.accept(secondSocket);
      first.accept(firstSocket);
      secondSocket.receive(JSON.stringify({ ...initializeRequest, id: 2 }));
      firstSocket.receive(JSON.stringify({ ...initializeRequest, id: 1 }));

      await Promise.all([
        readSentMessage(firstSocket),
        readSentMessage(secondSocket),
      ]);
      expect(createdBy).toEqual(expect.arrayContaining(["first", "second"]));
      expect(createdBy).toHaveLength(2);
    } finally {
      firstSocket.close();
      secondSocket.close();
      await server.close();
    }
  });

  it("removes rejected prepared WebSocket connections", async () => {
    const server = new AcpServer({
      createAgent: (conn) => new TestAgent(conn),
    });
    const prepared = server.prepareWebSocketUpgrade();

    try {
      prepared.reject();
      const response = await server.handleRequest(
        new Request("http://127.0.0.1/acp", {
          method: "GET",
          headers: {
            Accept: "text/event-stream",
            [HEADER_CONNECTION_ID]: prepared.connectionId,
          },
        }),
      );

      expect(response.status).toBe(404);
    } finally {
      await server.close();
    }
  });

  it("does not expose accepted WebSocket upgrades to HTTP before initialize succeeds", async () => {
    const server = new AcpServer({
      createAgent: (conn) => new TestAgent(conn),
    });
    const prepared = server.prepareWebSocketUpgrade();
    const socket = new FakeServerSocket();

    try {
      prepared.accept(socket);

      const getResponse = await server.handleRequest(
        new Request("http://127.0.0.1/acp", {
          method: "GET",
          headers: {
            Accept: "text/event-stream",
            [HEADER_CONNECTION_ID]: prepared.connectionId,
          },
        }),
      );
      const postResponse = await server.handleRequest(
        new Request("http://127.0.0.1/acp", {
          method: "POST",
          headers: {
            "Content-Type": JSON_MIME_TYPE,
            [HEADER_CONNECTION_ID]: prepared.connectionId,
          },
          body: JSON.stringify(sessionNewRequest),
        }),
      );
      const deleteResponse = await server.handleRequest(
        new Request("http://127.0.0.1/acp", {
          method: "DELETE",
          headers: {
            [HEADER_CONNECTION_ID]: prepared.connectionId,
          },
        }),
      );

      expect(getResponse.status).toBe(404);
      expect(postResponse.status).toBe(404);
      expect(deleteResponse.status).toBe(404);

      socket.receive(JSON.stringify(initializeRequest));
      await expect(readSentMessage(socket)).resolves.toMatchObject({
        jsonrpc: "2.0",
        id: initializeRequest.id,
        result: {
          protocolVersion: PROTOCOL_VERSION,
        },
      });
    } finally {
      socket.close();
      await server.close();
    }
  });

  it("keeps existing double-settle behavior for prepared WebSocket upgrades", async () => {
    const server = new AcpServer({
      createAgent: (conn) => new TestAgent(conn),
    });
    const rejected = server.prepareWebSocketUpgrade();
    const accepted = server.prepareWebSocketUpgrade();
    const socket = new FakeServerSocket();

    try {
      rejected.reject();
      expect(() => rejected.accept(new FakeServerSocket())).toThrow(
        "ACP WebSocket upgrade has already been settled",
      );

      accepted.accept(socket);
      expect(() => accepted.accept(new FakeServerSocket())).toThrow(
        "ACP WebSocket upgrade has already been settled",
      );
      expect(() => accepted.reject()).not.toThrow();
    } finally {
      socket.close();
      await server.close();
    }
  });
});

function recordingFactory(
  createdBy: string[],
  label: string,
): (conn: AgentSideConnection) => Agent {
  return (conn) => {
    createdBy.push(label);
    return new TestAgent(conn);
  };
}

function readSentMessage(socket: FakeServerSocket): Promise<AnyMessage> {
  const message = socket.sent.shift();

  if (message) {
    return Promise.resolve(JSON.parse(message));
  }

  return new Promise((resolve) => {
    socket.onSend = (data) => {
      resolve(JSON.parse(data));
    };
  });
}

class FakeServerSocket implements WebSocketServerSocket {
  readonly sent: string[] = [];
  readonly listeners = new Map<string, Set<(event: unknown) => void>>();
  onSend: ((data: string) => void) | undefined;

  send(data: string): void {
    this.sent.push(data);
    this.onSend?.(data);
    this.onSend = undefined;
  }

  close(_code?: number, _reason?: string): void {
    this.emit("close", {});
  }

  addEventListener(type: string, listener: (event: unknown) => void): void {
    this.listeners.set(type, this.listeners.get(type) ?? new Set());
    this.listeners.get(type)?.add(listener);
  }

  removeEventListener(type: string, listener: (event: unknown) => void): void {
    this.listeners.get(type)?.delete(listener);
  }

  receive(data: string): void {
    this.emit("message", { data });
  }

  private emit(type: string, event: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }
}
