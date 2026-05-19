#!/usr/bin/env node

import * as acp from "@agentclientprotocol/sdk";
import { createHttpStream } from "@agentclientprotocol/sdk/http-client";

class HttpExampleClient implements acp.Client {
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

const serverUrl = process.env.ACP_HTTP_URL ?? "http://127.0.0.1:7331/acp";
const stream = createHttpStream(serverUrl, {
  headers: {
    Authorization: "Bearer example-token",
  },
  // To use cookies, pass a cookie-aware fetch implementation here instead of relying on a built-in cookie jar.
  // fetch: cookieAwareFetch,
});
const connection = new acp.ClientSideConnection(
  (_agent) => new HttpExampleClient(),
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
        text: "Hello over Streamable HTTP",
      },
    ],
  });

  console.log(`\nDone: ${result.stopReason}`);
} finally {
  await stream.writable.close();
}
