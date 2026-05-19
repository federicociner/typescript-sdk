#!/usr/bin/env node

import { WebSocket } from "ws";

import * as acp from "@agentclientprotocol/sdk";
import { createWebSocketStream } from "@agentclientprotocol/sdk/ws-client";
import type { WebSocketConstructor } from "@agentclientprotocol/sdk/ws-client";

class WebSocketExampleClient implements acp.Client {
  async requestPermission(
    params: acp.RequestPermissionRequest,
  ): Promise<acp.RequestPermissionResponse> {
    return {
      outcome: {
        outcome: "selected",
        optionId: params.options[0]?.optionId ?? "allow",
      },
    };
  }

  async sessionUpdate(params: acp.SessionNotification): Promise<void> {
    const update = params.update;

    if (update.sessionUpdate === "agent_message_chunk") {
      process.stdout.write(
        update.content.type === "text" ? update.content.text : "",
      );
      return;
    }

    console.log(`[${update.sessionUpdate}]`);
  }
}

const serverUrl = process.env.ACP_WS_URL ?? "ws://127.0.0.1:7331/acp";
const stream = createWebSocketStream(serverUrl, {
  WebSocket: WebSocket satisfies WebSocketConstructor,
  // Custom headers work with Node's `ws` constructor. Browser WebSocket does not support custom headers.
  headers: {
    Authorization: "Bearer example-token",
  },
});
const connection = new acp.ClientSideConnection(
  (_agent) => new WebSocketExampleClient(),
  stream,
);

try {
  await connection.initialize({
    protocolVersion: acp.PROTOCOL_VERSION,
    clientCapabilities: {},
  });

  const session = await connection.newSession({
    cwd: process.cwd(),
    mcpServers: [],
  });

  const result = await connection.prompt({
    sessionId: session.sessionId,
    prompt: [
      {
        type: "text",
        text: "Hello over WebSocket",
      },
    ],
  });

  console.log(`\nDone: ${result.stopReason}`);
} finally {
  await stream.writable.close();
}
