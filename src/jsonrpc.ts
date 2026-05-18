/**
 * JSON-RPC 2.0 type definitions for internal use.
 */

export type AnyMessage = AnyRequest | AnyResponse | AnyNotification;

export type AnyRequest = {
  jsonrpc: "2.0";
  id: string | number | null;
  method: string;
  params?: unknown;
};

export type AnyResponse = {
  jsonrpc: "2.0";
  id: string | number | null;
} & Result<unknown>;

export type AnyNotification = {
  jsonrpc: "2.0";
  method: string;
  params?: unknown;
};

export type Result<T> =
  | {
      result: T;
    }
  | {
      error: ErrorResponse;
    };

export type ErrorResponse = {
  code: number;
  message: string;
  data?: unknown;
};

export type RequestHandler = (
  method: string,
  params: unknown,
) => Promise<unknown>;
export type NotificationHandler = (
  method: string,
  params: unknown,
) => Promise<void>;

export function isJsonRpcMessage(value: unknown): value is AnyMessage {
  if (!isRecord(value) || value["jsonrpc"] !== "2.0") {
    return false;
  }

  if ("method" in value) {
    return typeof value["method"] === "string";
  }

  return "id" in value;
}

export function isRequestMessage(message: AnyMessage): message is AnyRequest {
  return "id" in message && "method" in message;
}

export function isResponseMessage(message: AnyMessage): message is AnyResponse {
  return "id" in message && !("method" in message);
}

export function isNotificationMessage(
  message: AnyMessage,
): message is AnyNotification {
  return "method" in message && !("id" in message);
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
