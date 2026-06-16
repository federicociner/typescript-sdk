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
  return (
    isRequestMessage(value) ||
    isResponseMessage(value) ||
    isNotificationMessage(value)
  );
}

export function isRequestMessage(value: unknown): value is AnyRequest {
  return (
    isJsonRpcEnvelope(value) &&
    "id" in value &&
    typeof value["method"] === "string" &&
    isJsonRpcId(value["id"])
  );
}

export function isResponseMessage(value: unknown): value is AnyResponse {
  if (!isJsonRpcEnvelope(value) || "method" in value) {
    return false;
  }

  if (!("id" in value) || !isJsonRpcId(value["id"])) {
    return false;
  }

  const hasResult = Object.hasOwn(value, "result");
  const hasError = Object.hasOwn(value, "error");

  if (hasResult === hasError) {
    return false;
  }

  return !hasError || isErrorResponse(value["error"]);
}

export function isNotificationMessage(
  value: unknown,
): value is AnyNotification {
  return (
    isJsonRpcEnvelope(value) &&
    !("id" in value) &&
    typeof value["method"] === "string"
  );
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isJsonRpcEnvelope(
  value: unknown,
): value is Record<string, unknown> & { jsonrpc: "2.0" } {
  return isRecord(value) && value["jsonrpc"] === "2.0";
}

function isJsonRpcId(value: unknown): value is string | number | null {
  return (
    value === null ||
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value))
  );
}

function isErrorResponse(value: unknown): value is ErrorResponse {
  return (
    isRecord(value) &&
    typeof value["code"] === "number" &&
    Number.isInteger(value["code"]) &&
    typeof value["message"] === "string"
  );
}
