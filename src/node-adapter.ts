import { HEADER_CONNECTION_ID } from "./protocol.js";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import type { AcpServer } from "./server.js";
import type { WebSocketServer } from "ws";

export function createNodeHttpHandler(
  server: AcpServer,
): (req: IncomingMessage, res: ServerResponse) => void {
  return (req, res) => {
    void handleNodeRequest(server, req, res);
  };
}

export function createNodeWebSocketUpgradeHandler(
  server: AcpServer,
  webSocketServer: WebSocketServer,
): (req: IncomingMessage, socket: Duplex, head: Buffer) => void {
  return (req, socket, head) => {
    const upgrade = server.prepareWebSocketUpgrade();
    let hasAccepted = false;

    const cleanup = (): void => {
      webSocketServer.off("headers", onHeaders);
      socket.off("close", onUpgradeFailed);
      socket.off("error", onUpgradeFailed);
    };

    const onHeaders = (headers: string[], request: IncomingMessage): void => {
      if (request !== req) {
        return;
      }

      headers.push(`${HEADER_CONNECTION_ID}: ${upgrade.connectionId}`);
    };

    const onUpgradeFailed = (): void => {
      if (hasAccepted) {
        return;
      }

      cleanup();
      upgrade.reject();
    };

    webSocketServer.on("headers", onHeaders);
    socket.once("close", onUpgradeFailed);
    socket.once("error", onUpgradeFailed);

    try {
      webSocketServer.handleUpgrade(req, socket, head, (webSocket) => {
        hasAccepted = true;
        cleanup();
        upgrade.accept(webSocket);
      });
    } catch (error) {
      cleanup();
      upgrade.reject();
      throw error;
    }
  };
}

async function handleNodeRequest(
  server: AcpServer,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<void> {
  try {
    await writeNodeResponse(
      res,
      await server.handleRequest(await toWebRequest(req)),
    );
  } catch (error) {
    if (!res.headersSent) {
      res.statusCode = 500;
      res.setHeader("Content-Type", "text/plain");
    }

    res.end(error instanceof Error ? error.message : "Internal Server Error");
  }
}

async function toWebRequest(req: IncomingMessage): Promise<Request> {
  return new Request(nodeRequestUrl(req), {
    method: req.method ?? "GET",
    headers: nodeHeaders(req),
    body: hasRequestBody(req) ? await readRequestBody(req) : undefined,
  });
}

function hasRequestBody(req: IncomingMessage): boolean {
  return req.method !== "GET" && req.method !== "HEAD";
}

async function readRequestBody(req: IncomingMessage): Promise<string> {
  const chunks: string[] = [];

  for await (const chunk of req) {
    chunks.push(
      typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8"),
    );
  }

  return chunks.join("");
}

function nodeRequestUrl(req: IncomingMessage): string {
  const host = req.headers.host ?? "localhost";
  return `http://${host}${req.url ?? "/"}`;
}

function nodeHeaders(req: IncomingMessage): Headers {
  const headers = new Headers();

  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(name, item);
      }

      continue;
    }

    if (value !== undefined) {
      headers.set(name, value);
    }
  }

  return headers;
}

async function writeNodeResponse(
  res: ServerResponse,
  response: Response,
): Promise<void> {
  res.statusCode = response.status;

  response.headers.forEach((value, name) => {
    res.setHeader(name, value);
  });

  res.flushHeaders();

  const responseBody = response.body;

  if (!responseBody) {
    res.end();
    return;
  }

  const reader = responseBody.getReader();

  try {
    while (true) {
      const result = await reader.read();

      if (result.done) {
        res.end();
        return;
      }

      await writeChunk(res, result.value);
    }
  } finally {
    reader.releaseLock();
  }
}

function writeChunk(res: ServerResponse, chunk: Uint8Array): Promise<void> {
  return new Promise((resolve, reject) => {
    const onError = (error: Error): void => {
      res.off("drain", onDrain);
      reject(error);
    };

    const onDrain = (): void => {
      res.off("error", onError);
      resolve();
    };

    res.once("error", onError);

    if (res.write(chunk)) {
      res.off("error", onError);
      resolve();
      return;
    }

    res.once("drain", onDrain);
  });
}
