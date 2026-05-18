import http from "node:http";

import { describe, expect, it } from "vitest";
import { AcpServer } from "./server.js";
import { createNodeHttpHandler } from "./node-adapter.js";
import { TestAgent } from "./test-support/test-agent.js";

import type { AgentSideConnection } from "./acp.js";

interface RunningServer {
  readonly url: string;
  readonly close: () => Promise<void>;
}

describe("createNodeHttpHandler", () => {
  it("forwards method, URL, headers, and body to AcpServer.handleRequest", async () => {
    const acpServer = new AcpServer({
      createAgent: (conn: AgentSideConnection) => new TestAgent(conn),
    });
    const seenRequests: Request[] = [];
    const seenBodies: string[] = [];
    acpServer.handleRequest = async (req) => {
      seenRequests.push(req);
      seenBodies.push(await req.text());
      return new Response("created", {
        status: 201,
        headers: {
          "X-Adapter-Test": "ok",
        },
      });
    };

    const server = await startNodeServer(acpServer);

    try {
      const response = await fetch(`${server.url}/acp?hello=world`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Client-Test": "forwarded",
        },
        body: JSON.stringify({ ok: true }),
      });

      expect(response.status).toBe(201);
      expect(response.headers.get("X-Adapter-Test")).toBe("ok");
      expect(await response.text()).toBe("created");
      expect(seenRequests).toHaveLength(1);
      expect(seenRequests[0]?.method).toBe("POST");
      expect(seenRequests[0]?.url).toBe(`${server.url}/acp?hello=world`);
      expect(seenRequests[0]?.headers.get("Content-Type")).toBe(
        "application/json",
      );
      expect(seenRequests[0]?.headers.get("X-Client-Test")).toBe("forwarded");
      expect(seenBodies).toEqual([JSON.stringify({ ok: true })]);
    } finally {
      await server.close();
    }
  });

  it("streams response bodies to ServerResponse", async () => {
    const acpServer = new AcpServer({
      createAgent: (conn: AgentSideConnection) => new TestAgent(conn),
    });
    acpServer.handleRequest = () =>
      Promise.resolve(
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              const encoder = new TextEncoder();
              controller.enqueue(encoder.encode("data: one\n\n"));
              controller.enqueue(encoder.encode("data: two\n\n"));
              controller.close();
            },
          }),
          {
            status: 200,
            headers: {
              "Content-Type": "text/event-stream",
            },
          },
        ),
      );

    const server = await startNodeServer(acpServer);

    try {
      const response = await fetch(server.url, { method: "POST" });

      expect(response.status).toBe(200);
      expect(response.headers.get("Content-Type")).toContain(
        "text/event-stream",
      );
      expect(await response.text()).toBe("data: one\n\ndata: two\n\n");
    } finally {
      await server.close();
    }
  });

  it("handles empty response bodies", async () => {
    const acpServer = new AcpServer({
      createAgent: (conn: AgentSideConnection) => new TestAgent(conn),
    });
    acpServer.handleRequest = () =>
      Promise.resolve(
        new Response(null, {
          status: 202,
          headers: {
            "X-Empty-Body": "yes",
          },
        }),
      );

    const server = await startNodeServer(acpServer);

    try {
      const response = await fetch(server.url, { method: "POST" });

      expect(response.status).toBe(202);
      expect(response.headers.get("X-Empty-Body")).toBe("yes");
      expect(await response.text()).toBe("");
    } finally {
      await server.close();
    }
  });
});

async function startNodeServer(acpServer: AcpServer): Promise<RunningServer> {
  const server = http.createServer(createNodeHttpHandler(acpServer));

  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error): void => {
      server.off("listening", onListening);
      reject(error);
    };

    const onListening = (): void => {
      server.off("error", onError);
      resolve();
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(0, "127.0.0.1");
  });

  const address = server.address();

  if (typeof address !== "object" || address === null) {
    throw new Error("Node test server did not bind to a TCP port");
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      }),
  };
}
