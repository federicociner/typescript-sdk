import http from "node:http";

import { AcpServer } from "../server.js";
import { createNodeHttpHandler } from "../node-adapter.js";
import { TestAgent } from "./test-agent.js";

import type { AddressInfo } from "node:net";
import type { Agent, AgentSideConnection } from "../acp.js";

export interface TestHttpServer {
  readonly url: string;
  readonly close: () => Promise<void>;
}

export async function startTestServer(
  agentFactory: (conn: AgentSideConnection) => Agent = (conn) =>
    new TestAgent(conn),
  options: { port?: number } = {},
): Promise<TestHttpServer> {
  const acpServer = new AcpServer({ createAgent: agentFactory });
  const httpServer = http.createServer(createNodeHttpHandler(acpServer));

  await listen(httpServer, options.port ?? 0);

  const address = httpServer.address();

  if (!isAddressInfo(address)) {
    throw new Error("Test HTTP server did not bind to a TCP port");
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      await Promise.all([acpServer.close(), closeHttpServer(httpServer)]);
    },
  };
}

function listen(server: http.Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
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
    server.listen(port, "127.0.0.1");
  });
}

function closeHttpServer(server: http.Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
        return;
      }

      resolve();
    });
  });
}

function isAddressInfo(
  address: ReturnType<http.Server["address"]>,
): address is AddressInfo {
  return typeof address === "object" && address !== null;
}
