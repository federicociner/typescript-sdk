#!/usr/bin/env node

import { createServer } from "node:http";

import { WebSocketServer } from "ws";

import * as acp from "@agentclientprotocol/sdk";
import { createNodeHttpHandler } from "@agentclientprotocol/sdk/node";
import { AcpServer } from "@agentclientprotocol/sdk/server";

class HttpExampleAgent implements acp.Agent {
  private readonly connection: acp.AgentSideConnection;
  private readonly sessions = new Set<string>();

  constructor(connection: acp.AgentSideConnection) {
    this.connection = connection;
  }

  async initialize(
    _params: acp.InitializeRequest,
  ): Promise<acp.InitializeResponse> {
    return {
      protocolVersion: acp.PROTOCOL_VERSION,
      agentCapabilities: {
        loadSession: false,
      },
    };
  }

  async newSession(
    _params: acp.NewSessionRequest,
  ): Promise<acp.NewSessionResponse> {
    const sessionId = crypto.randomUUID();
    this.sessions.add(sessionId);
    return { sessionId };
  }

  async authenticate(
    _params: acp.AuthenticateRequest,
  ): Promise<acp.AuthenticateResponse> {
    return {};
  }

  async prompt(params: acp.PromptRequest): Promise<acp.PromptResponse> {
    if (!this.sessions.has(params.sessionId)) {
      throw new Error(`Session ${params.sessionId} not found`);
    }

    await this.connection.sessionUpdate({
      sessionId: params.sessionId,
      update: {
        sessionUpdate: "agent_message_chunk",
        content: {
          type: "text",
          text: "Hello from the ACP HTTP/WebSocket example server.",
        },
      },
    });

    return { stopReason: "end_turn" };
  }

  async cancel(_params: acp.CancelNotification): Promise<void> {}
}

const acpServer = new AcpServer({
  createAgent: (connection) => new HttpExampleAgent(connection),
});
const acpHttpHandler = createNodeHttpHandler(acpServer);
const webSocketServer = new WebSocketServer({ noServer: true });
const port = Number.parseInt(process.env.PORT ?? "7331", 10);

const httpServer = createServer((req, res) => {
  if (!isAcpPath(req.url)) {
    res.writeHead(404, { "Content-Type": "text/plain" });
    res.end("Not Found");
    return;
  }

  // Put authentication or tenant-selection middleware here before routing to AcpServer.
  // For example, validate `req.headers.authorization` and reject unauthorized requests.
  if (!isAuthorized(req.headers.authorization)) {
    res.writeHead(401, { "Content-Type": "text/plain" });
    res.end("Unauthorized");
    return;
  }

  acpHttpHandler(req, res);
});

httpServer.on("upgrade", (req, socket, head) => {
  if (!isAcpPath(req.url) || !isAuthorized(req.headers.authorization)) {
    socket.destroy();
    return;
  }

  webSocketServer.handleUpgrade(req, socket, head, (ws) => {
    acpServer.handleWebSocket(ws);
  });
});

httpServer.listen(port, () => {
  console.log(`ACP HTTP endpoint listening at http://127.0.0.1:${port}/acp`);
  console.log(`ACP WebSocket endpoint listening at ws://127.0.0.1:${port}/acp`);
});

function isAcpPath(url: string | undefined): boolean {
  return new URL(url ?? "/", "http://127.0.0.1").pathname === "/acp";
}

function isAuthorized(authorization: string | undefined): boolean {
  return (
    authorization === undefined || authorization === "Bearer example-token"
  );
}
