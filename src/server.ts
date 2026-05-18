import { ConnectionRegistry } from "./connection.js";
import {
  HEADER_CONNECTION_ID,
  JSON_MIME_TYPE,
  isInitializeRequest,
} from "./protocol.js";

import type { Agent, AgentSideConnection } from "./acp.js";
import type { AnyMessage } from "./jsonrpc.js";

export interface AcpServerOptions {
  createAgent: (conn: AgentSideConnection) => Agent;
}

export class AcpServer {
  private readonly createAgent: (conn: AgentSideConnection) => Agent;
  private readonly registry = new ConnectionRegistry();

  constructor(options: AcpServerOptions) {
    this.createAgent = options.createAgent;
  }

  async handleRequest(req: Request): Promise<Response> {
    if (req.method !== "POST") {
      return textResponse("Method Not Allowed", 405);
    }

    const contentType = req.headers.get("Content-Type");

    if (!contentType?.startsWith(JSON_MIME_TYPE)) {
      return textResponse("Unsupported Media Type", 415);
    }

    const body = await readJson(req);

    if (!body.ok) {
      return textResponse("Invalid JSON", 400);
    }

    if (Array.isArray(body.value)) {
      return textResponse("Batch JSON-RPC requests are not implemented", 501);
    }

    if (!isJsonRpcMessage(body.value)) {
      return textResponse("Invalid JSON-RPC message", 400);
    }

    const connectionId = req.headers.get(HEADER_CONNECTION_ID);

    if (isInitializeRequest(body.value) && !connectionId) {
      return await this.handleInitialize(body.value);
    }

    if (!connectionId) {
      return textResponse("Missing Acp-Connection-Id", 400);
    }

    if (!this.registry.get(connectionId)) {
      return textResponse("Unknown Acp-Connection-Id", 404);
    }

    return textResponse(
      "Connected POST handling is not implemented in Phase 1",
      400,
    );
  }

  async close(): Promise<void> {
    this.registry.closeAll();
  }

  private async handleInitialize(message: AnyMessage): Promise<Response> {
    if (!("id" in message) || message.id === null) {
      return textResponse("Initialize request must include an ID", 400);
    }

    let connection:
      | ReturnType<ConnectionRegistry["createConnection"]>
      | undefined;

    try {
      connection = this.registry.createConnection(this.createAgent);
      const writer = connection.inboundTx.getWriter();

      try {
        await writer.write(message);
      } finally {
        writer.releaseLock();
      }

      const initialResponse = await connection.recvInitial(message.id);

      return jsonResponse(initialResponse, 200, {
        [HEADER_CONNECTION_ID]: connection.connectionId,
      });
    } catch (error) {
      if (connection) {
        this.registry.remove(connection.connectionId);
      }

      return jsonResponse(
        {
          jsonrpc: "2.0",
          id: message.id,
          error: {
            code: -32603,
            message: "Initialize failed",
            data: error instanceof Error ? error.message : undefined,
          },
        },
        500,
      );
    }
  }
}

type JsonResult =
  | {
      ok: true;
      value: unknown;
    }
  | {
      ok: false;
    };

async function readJson(req: Request): Promise<JsonResult> {
  try {
    return {
      ok: true,
      value: await req.json(),
    };
  } catch {
    return {
      ok: false,
    };
  }
}

function isJsonRpcMessage(value: unknown): value is AnyMessage {
  return (
    isRecord(value) &&
    value.jsonrpc === "2.0" &&
    ("method" in value || "id" in value)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function jsonResponse(
  value: unknown,
  status: number,
  headers?: HeadersInit,
): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "Content-Type": JSON_MIME_TYPE,
      ...headers,
    },
  });
}

function textResponse(body: string, status: number): Response {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/plain",
    },
  });
}
