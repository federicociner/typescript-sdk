import { PROTOCOL_VERSION } from "../schema/index.js";

import type {
  Agent,
  AgentSideConnection,
  AuthenticateRequest,
  AuthenticateResponse,
  CancelNotification,
  InitializeRequest,
  InitializeResponse,
  NewSessionRequest,
  NewSessionResponse,
  PromptRequest,
  PromptResponse,
} from "../acp.js";

export interface TestAgentOptions {
  readonly chunkCount?: number;
  readonly chunkDelayMs?: number;
}

export class TestAgent implements Agent {
  private readonly connection: AgentSideConnection;
  private readonly chunkCount: number;
  private readonly chunkDelayMs: number;

  constructor(connection: AgentSideConnection, options: TestAgentOptions = {}) {
    this.connection = connection;
    this.chunkCount = options.chunkCount ?? 1;
    this.chunkDelayMs = options.chunkDelayMs ?? 0;
  }

  initialize(_params: InitializeRequest): Promise<InitializeResponse> {
    return Promise.resolve({
      protocolVersion: PROTOCOL_VERSION,
      agentCapabilities: {
        loadSession: false,
      },
    });
  }

  newSession(_params: NewSessionRequest): Promise<NewSessionResponse> {
    return Promise.resolve({ sessionId: globalThis.crypto.randomUUID() });
  }

  authenticate(
    _params: AuthenticateRequest,
  ): Promise<AuthenticateResponse | void> {
    return Promise.resolve();
  }

  async prompt(params: PromptRequest): Promise<PromptResponse> {
    for (const index of Array.from(
      { length: this.chunkCount },
      (_, item) => item,
    )) {
      if (this.chunkDelayMs > 0) {
        await delay(this.chunkDelayMs);
      }

      await this.connection.sessionUpdate({
        sessionId: params.sessionId,
        update: {
          sessionUpdate: "agent_message_chunk",
          content: {
            type: "text",
            text: `chunk-${index + 1}`,
          },
        },
      });
    }

    return { stopReason: "end_turn" };
  }

  cancel(_params: CancelNotification): Promise<void> {
    return Promise.resolve();
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
