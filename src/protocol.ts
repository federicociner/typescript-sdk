import { AGENT_METHODS } from "./schema/index.js";

import type { AnyMessage } from "./jsonrpc.js";

export const HEADER_CONNECTION_ID = "Acp-Connection-Id";
export const HEADER_SESSION_ID = "Acp-Session-Id";
export const EVENT_STREAM_MIME_TYPE = "text/event-stream";
export const JSON_MIME_TYPE = "application/json";

const SESSION_SCOPED_METHODS = new Set<string>([
  AGENT_METHODS.session_cancel,
  AGENT_METHODS.session_close,
  AGENT_METHODS.session_load,
  AGENT_METHODS.session_prompt,
  AGENT_METHODS.session_resume,
  AGENT_METHODS.session_set_config_option,
  AGENT_METHODS.session_set_mode,
  AGENT_METHODS.session_set_model,
]);

export function methodRequiresSessionHeader(method: string): boolean {
  return SESSION_SCOPED_METHODS.has(method);
}

export function sessionIdFromParams(params: unknown): string | undefined {
  if (!isRecord(params)) {
    return undefined;
  }

  const sessionId = params["sessionId"];
  return typeof sessionId === "string" ? sessionId : undefined;
}

export function isInitializeRequest(msg: AnyMessage): boolean {
  return (
    msg.jsonrpc === "2.0" &&
    "id" in msg &&
    "method" in msg &&
    msg.method === AGENT_METHODS.initialize
  );
}

export function messageIdKey(
  id: string | number | null | undefined,
): string | undefined {
  if (typeof id === "string") {
    return `string:${id}`;
  }

  if (typeof id === "number") {
    return `number:${id}`;
  }

  return undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
