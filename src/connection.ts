import { AgentSideConnection } from "./acp.js";

import type { Agent } from "./acp.js";
import type { AnyMessage, AnyResponse } from "./jsonrpc.js";
import type { Stream } from "./stream.js";

export class ConnectionState {
  readonly connectionId: string;
  readonly inboundTx: WritableStream<AnyMessage>;
  readonly outboundRx: ReadableStream<AnyMessage>;
  readonly agentConnection: AgentSideConnection;

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

  async shutdown() {
    await Promise.allSettled([
      this.inboundTx.close(),
      this.outboundRx.cancel(),
    ]);
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

function isMatchingResponse(
  msg: AnyMessage,
  id: string | number,
): msg is AnyResponse {
  return "id" in msg && !("method" in msg) && msg.id === id;
}
