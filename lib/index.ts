import type { Exchange, Operation, OperationResult } from "@urql/core";
import { CombinedError, createRequest, stringifyDocument } from "@urql/core";
import {
  filter,
  makeSubject,
  merge,
  onEnd,
  onPush,
  pipe,
  subscribe,
} from "wonka";

export interface CrossTabSyncOptions {
  /** BroadcastChannel name. All tabs sharing this name participate in the same sync group. */
  channelName?: string;
  /** Identifier for this tab. Defaults to a random UUID. Used to drop echoes from self. */
  tabId?: string;
  /** Predicate to decide whether an operation should sync. Defaults to `() => true`. */
  shouldSync?: (operation: Operation) => boolean;
  /** Whether to mirror mutations (start, result, error). Default: `true`. */
  syncMutations?: boolean;
  /** Whether to broadcast successful query results so other tabs can warm their cache. Default: `true`. */
  syncQueries?: boolean;
}

interface CrossTabMeta {
  remote: true;
  txId: string;
}

type Message =
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

interface SerializedError {
  networkError?: { name: string; message: string };
  graphQLErrors?: Array<{
    message: string;
    path?: ReadonlyArray<string | number>;
    extensions?: Record<string, unknown>;
  }>;
}

function generateId(): string {
  const c =
    typeof globalThis !== "undefined"
      ? (globalThis as { crypto?: { randomUUID?: () => string } }).crypto
      : undefined;
  if (c?.randomUUID) return c.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

function serializeError(error: CombinedError): SerializedError {
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

function deserializeError(payload: SerializedError): CombinedError {
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

function isBrowser(): boolean {
  return (
    typeof globalThis !== "undefined" &&
    typeof (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel !==
      "undefined"
  );
}

function getCrossTabMeta(operation: Operation): CrossTabMeta | undefined {
  return (operation.context as { crossTabSync?: CrossTabMeta }).crossTabSync;
}

function correlationKey(op: Operation): string {
  const inst = (op.context as { _instance?: unknown })._instance;
  return inst !== undefined ? `i:${String(inst)}` : `k:${op.key}`;
}

export function crossTabSyncExchange(
  options: CrossTabSyncOptions = {},
): Exchange {
  const {
    channelName = "urql-crosstab-sync",
    tabId = generateId(),
    shouldSync = () => true,
    syncMutations = true,
    syncQueries = true,
  } = options;

  if (!isBrowser()) {
    return ({ forward }) =>
      (ops$) =>
        forward(ops$);
  }

  return ({ client, forward }) => {
    const channel = new BroadcastChannel(channelName);
    const remoteResults$ = makeSubject<OperationResult>();

    // Outgoing correlation: correlationKey(op) -> txId for our locally-initiated mutations
    const outgoingMutations = new Map<string, string>();
    // Synthetic remote ops awaiting a result message. Set when the op flows through us.
    const pendingRemoteOps = new Map<string, Operation>();
    // Result messages received before their synthetic op has been observed in our stream.
    const earlyResults = new Map<string, (op: Operation) => OperationResult>();
    // Keep the executeRequestOperation subscriptions alive until we emit a result.
    const subscriptions = new Map<string, () => void>();

    function deliverResult(
      txId: string,
      build: (op: Operation) => OperationResult,
    ) {
      const op = pendingRemoteOps.get(txId);
      if (op) {
        pendingRemoteOps.delete(txId);
        remoteResults$.next(build(op));
        const teardown = subscriptions.get(txId);
        subscriptions.delete(txId);
        if (teardown) {
          // Defer teardown so the result has time to propagate up through graphcache
          // and reach the executeRequestOperation subscriber.
          setTimeout(teardown, 0);
        }
      } else {
        earlyResults.set(txId, build);
      }
    }

    function dispatchRemoteOperation(
      kind: "mutation" | "query",
      query: string,
      variables: Record<string, unknown>,
      txId: string,
    ) {
      let req: ReturnType<typeof createRequest>;
      try {
        req = createRequest(query, variables);
      } catch {
        return;
      }
      const op = client.createRequestOperation(kind, req, {
        crossTabSync: { remote: true, txId },
        ...(kind === "query" ? { requestPolicy: "network-only" as const } : {}),
      });
      const sub = pipe(
        client.executeRequestOperation(op),
        subscribe(() => {
          // No-op: graphcache consumes the result and notifies its subscribers.
          // The sub itself keeps the operation "active" so urql doesn't tear it down early.
        }),
      );
      subscriptions.set(txId, () => sub.unsubscribe());
    }

    channel.addEventListener("message", (event: MessageEvent<Message>) => {
      const msg = event.data;
      if (
        !msg ||
        typeof msg !== "object" ||
        !("type" in msg) ||
        msg.tabId === tabId
      ) {
        return;
      }

      switch (msg.type) {
        case "mutation:start": {
          if (!syncMutations) return;
          dispatchRemoteOperation(
            "mutation",
            msg.query,
            msg.variables,
            msg.txId,
          );
          break;
        }
        case "mutation:result": {
          if (!syncMutations) return;
          deliverResult(msg.txId, (op) => ({
            operation: op,
            data: msg.data,
            error: undefined,
            extensions: msg.extensions,
            stale: false,
            hasNext: false,
          }));
          break;
        }
        case "mutation:error": {
          if (!syncMutations) return;
          deliverResult(msg.txId, (op) => ({
            operation: op,
            data: undefined,
            error: deserializeError(msg.error),
            extensions: undefined,
            stale: false,
            hasNext: false,
          }));
          break;
        }
        case "query:result": {
          if (!syncQueries) return;
          const txId = generateId();
          // Stash the result first so it's delivered as soon as the synthetic op
          // is observed flowing through this exchange.
          earlyResults.set(txId, (op) => ({
            operation: op,
            data: msg.data,
            error: undefined,
            extensions: msg.extensions,
            stale: false,
            hasNext: false,
          }));
          dispatchRemoteOperation("query", msg.query, msg.variables, txId);
          break;
        }
      }
    });

    return (ops$) => {
      const observed$ = pipe(
        ops$,
        onPush((op) => {
          // Capture synthetic remote ops so we can route their results.
          const meta = getCrossTabMeta(op);
          if (!meta?.remote) return;
          const txId = meta.txId;

          if (op.kind === "teardown") {
            pendingRemoteOps.delete(txId);
            earlyResults.delete(txId);
            const teardown = subscriptions.get(txId);
            subscriptions.delete(txId);
            if (teardown) teardown();
            return;
          }

          const buildResult = earlyResults.get(txId);
          if (buildResult) {
            earlyResults.delete(txId);
            // Defer so graphcache has finished processing the op (e.g. running
            // optimistic config) before it sees the result coming back.
            queueMicrotask(() => {
              remoteResults$.next(buildResult(op));
              const teardown = subscriptions.get(txId);
              subscriptions.delete(txId);
              if (teardown) setTimeout(teardown, 0);
            });
          } else {
            // Mutation case: store the op and wait for the result message.
            pendingRemoteOps.set(txId, op);
          }
        }),
      );

      const forwardable$ = pipe(
        observed$,
        // Short-circuit remote-marked ops away from the next exchange (fetch).
        // Teardowns of any kind still need to be forwarded so that the rest of
        // the pipeline can clean up associated state.
        filter((op) => {
          if (op.kind === "teardown") return true;
          return !getCrossTabMeta(op)?.remote;
        }),
        onPush((op) => {
          if (op.kind !== "mutation") return;
          if (!syncMutations) return;
          if (!shouldSync(op)) return;
          if (getCrossTabMeta(op)?.remote) return;

          const txId = generateId();
          outgoingMutations.set(correlationKey(op), txId);

          try {
            channel.postMessage({
              type: "mutation:start",
              tabId,
              txId,
              query: stringifyDocument(op.query),
              variables:
                (op.variables as Record<string, unknown> | undefined) ?? {},
            } satisfies Message);
          } catch (err) {
            outgoingMutations.delete(correlationKey(op));
            console.warn(
              "[urql-crosstab-sync] failed to broadcast mutation:start",
              err,
            );
          }
        }),
      );

      const forwarded$ = pipe(
        forward(forwardable$),
        onPush((result) => {
          const op = result.operation;
          const meta = getCrossTabMeta(op);
          if (meta?.remote) return; // never re-broadcast remote-originated results

          if (op.kind === "mutation") {
            if (!syncMutations) return;
            const cKey = correlationKey(op);
            const txId = outgoingMutations.get(cKey);
            if (!txId) return;
            outgoingMutations.delete(cKey);

            try {
              if (result.error) {
                channel.postMessage({
                  type: "mutation:error",
                  tabId,
                  txId,
                  error: serializeError(result.error),
                } satisfies Message);
              } else {
                channel.postMessage({
                  type: "mutation:result",
                  tabId,
                  txId,
                  data: result.data,
                  extensions: result.extensions,
                } satisfies Message);
              }
            } catch (err) {
              console.warn(
                "[urql-crosstab-sync] failed to broadcast mutation completion",
                err,
              );
            }
            return;
          }

          if (op.kind === "query") {
            if (!syncQueries) return;
            if (result.error) return;
            if (!shouldSync(op)) return;
            try {
              channel.postMessage({
                type: "query:result",
                tabId,
                query: stringifyDocument(op.query),
                variables:
                  (op.variables as Record<string, unknown> | undefined) ?? {},
                data: result.data,
                extensions: result.extensions,
              } satisfies Message);
            } catch (err) {
              console.warn(
                "[urql-crosstab-sync] failed to broadcast query:result",
                err,
              );
            }
          }
        }),
      );

      return pipe(
        merge([forwarded$, remoteResults$.source]),
        onEnd(() => {
          channel.close();
          outgoingMutations.clear();
          pendingRemoteOps.clear();
          earlyResults.clear();
          for (const teardown of subscriptions.values()) teardown();
          subscriptions.clear();
        }),
      );
    };
  };
}
