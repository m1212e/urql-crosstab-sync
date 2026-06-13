import { CombinedError } from "@urql/core";

/**
 * Wire format used over the `BroadcastChannel`. All fields must be
 * structured-cloneable; values flow through `postMessage` as-is.
 */
export type Message =
  | {
      type: "mutation:start";
      tabId: string;
      txId: string;
      query: string;
      variables: Record<string, unknown>;
    }
  | {
      type: "mutation:result";
      tabId: string;
      txId: string;
      data: unknown;
      extensions?: Record<string, unknown>;
    }
  | {
      type: "mutation:error";
      tabId: string;
      txId: string;
      error: SerializedError;
    }
  | {
      type: "query:result";
      tabId: string;
      query: string;
      variables: Record<string, unknown>;
      data: unknown;
      extensions?: Record<string, unknown>;
    };

export interface SerializedError {
  networkError?: { name: string; message: string };
  graphQLErrors?: Array<{
    message: string;
    path?: ReadonlyArray<string | number>;
    extensions?: Record<string, unknown>;
  }>;
}

export function serializeError(error: CombinedError): SerializedError {
  return {
    networkError: error.networkError
      ? { name: error.networkError.name, message: error.networkError.message }
      : undefined,
    graphQLErrors: error.graphQLErrors?.map((e) => ({
      message: e.message,
      path: e.path ?? undefined,
      extensions: e.extensions ?? undefined,
    })),
  };
}

export function deserializeError(payload: SerializedError): CombinedError {
  let networkError: Error | undefined;
  if (payload.networkError) {
    networkError = new Error(payload.networkError.message);
    networkError.name = payload.networkError.name;
  }
  return new CombinedError({
    networkError,
    graphQLErrors: payload.graphQLErrors as
      | Array<{ message: string }>
      | undefined,
  });
}

/** Type guard for messages received on the channel. */
export function isMessage(value: unknown): value is Message {
  return (
    typeof value === "object" &&
    value !== null &&
    "type" in value &&
    typeof (value as { type: unknown }).type === "string"
  );
}
