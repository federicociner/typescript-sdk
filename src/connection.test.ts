import { describe, expect, it } from "vitest";
import { ConnectionRegistry } from "./connection.js";
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
} as const;

describe("ConnectionRegistry", () => {
  it("creates retrievable connections with unique UUID connection IDs", () => {
    const registry = new ConnectionRegistry();
    const first = registry.createConnection(
      (conn: AgentSideConnection) => new TestAgent(conn),
    );
    const second = registry.createConnection(
      (conn: AgentSideConnection) => new TestAgent(conn),
    );

    expect(first.connectionId).toMatch(/^[0-9a-f-]{36}$/);
    expect(second.connectionId).toMatch(/^[0-9a-f-]{36}$/);
    expect(first.connectionId).not.toBe(second.connectionId);
    expect(registry.get(first.connectionId)).toBe(first);
    expect(registry.get(second.connectionId)).toBe(second);

    registry.closeAll();
  });

  it("removes connections", () => {
    const registry = new ConnectionRegistry();
    const connection = registry.createConnection(
      (conn: AgentSideConnection) => new TestAgent(conn),
    );

    expect(registry.remove(connection.connectionId)).toBe(connection);
    expect(registry.get(connection.connectionId)).toBeUndefined();
    expect(registry.remove(connection.connectionId)).toBeUndefined();
  });

  it("receives the initialize response directly from the agent", async () => {
    const registry = new ConnectionRegistry();
    const connection = registry.createConnection(
      (conn: AgentSideConnection) => new TestAgent(conn),
    );
    const writer = connection.inboundTx.getWriter();

    try {
      await writer.write(initializeRequest);
    } finally {
      writer.releaseLock();
    }

    const response = await connection.recvInitial(initializeRequest.id);

    expect(response).toMatchObject({
      jsonrpc: "2.0",
      id: initializeRequest.id,
      result: {
        protocolVersion: 1,
        agentCapabilities: {
          loadSession: false,
        },
      },
    });

    registry.closeAll();
  });
});
