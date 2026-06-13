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

//#region lib/index.ts
function generateId() {
  const c = typeof globalThis !== "undefined" ? globalThis.crypto : void 0;
  if (c?.randomUUID) return c.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
function serializeError(error) {
  return {
    networkError: error.networkError
      ? {
          name: error.networkError.name,
          message: error.networkError.message,
        }
      : void 0,
    graphQLErrors: error.graphQLErrors?.map((e) => ({
      message: e.message,
      path: e.path ?? void 0,
      extensions: e.extensions ?? void 0,
    })),
  };
}
function deserializeError(payload) {
  let networkError;
  if (payload.networkError) {
    networkError = new Error(payload.networkError.message);
    networkError.name = payload.networkError.name;
  }
  return new CombinedError({
    networkError,
    graphQLErrors: payload.graphQLErrors,
  });
}
function isBrowser() {
  return (
    typeof globalThis !== "undefined" &&
    typeof globalThis.BroadcastChannel !== "undefined"
  );
}
function getCrossTabMeta(operation) {
  return operation.context.crossTabSync;
}
function correlationKey(op) {
  const inst = op.context._instance;
  return inst !== void 0 ? `i:${String(inst)}` : `k:${op.key}`;
}
function crossTabSyncExchange(options = {}) {
  const {
    channelName = "urql-crosstab-sync",
    tabId = generateId(),
    shouldSync = () => true,
    syncMutations = true,
    syncQueries = true,
  } = options;
  if (!isBrowser())
    return ({ forward }) =>
      (ops$) =>
        forward(ops$);
  return ({ client, forward }) => {
    const channel = new BroadcastChannel(channelName);
    const remoteResults$ = makeSubject();
    const outgoingMutations = /* @__PURE__ */ new Map();
    const pendingRemoteOps = /* @__PURE__ */ new Map();
    const earlyResults = /* @__PURE__ */ new Map();
    const subscriptions = /* @__PURE__ */ new Map();
    function deliverResult(txId, build) {
      const op = pendingRemoteOps.get(txId);
      if (op) {
        pendingRemoteOps.delete(txId);
        remoteResults$.next(build(op));
        const teardown = subscriptions.get(txId);
        subscriptions.delete(txId);
        if (teardown) setTimeout(teardown, 0);
      } else earlyResults.set(txId, build);
    }
    function dispatchRemoteOperation(kind, query, variables, txId) {
      let req;
      try {
        req = createRequest(query, variables);
      } catch {
        return;
      }
      const op = client.createRequestOperation(kind, req, {
        crossTabSync: {
          remote: true,
          txId,
        },
        ...(kind === "query" ? { requestPolicy: "network-only" } : {}),
      });
      const sub = pipe(
        client.executeRequestOperation(op),
        subscribe(() => {}),
      );
      subscriptions.set(txId, () => sub.unsubscribe());
    }
    channel.addEventListener("message", (event) => {
      const msg = event.data;
      if (
        !msg ||
        typeof msg !== "object" ||
        !("type" in msg) ||
        msg.tabId === tabId
      )
        return;
      switch (msg.type) {
        case "mutation:start":
          if (!syncMutations) return;
          dispatchRemoteOperation(
            "mutation",
            msg.query,
            msg.variables,
            msg.txId,
          );
          break;
        case "mutation:result":
          if (!syncMutations) return;
          deliverResult(msg.txId, (op) => ({
            operation: op,
            data: msg.data,
            error: void 0,
            extensions: msg.extensions,
            stale: false,
            hasNext: false,
          }));
          break;
        case "mutation:error":
          if (!syncMutations) return;
          deliverResult(msg.txId, (op) => ({
            operation: op,
            data: void 0,
            error: deserializeError(msg.error),
            extensions: void 0,
            stale: false,
            hasNext: false,
          }));
          break;
        case "query:result": {
          if (!syncQueries) return;
          const txId = generateId();
          earlyResults.set(txId, (op) => ({
            operation: op,
            data: msg.data,
            error: void 0,
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
      return pipe(
        merge([
          pipe(
            forward(
              pipe(
                pipe(
                  ops$,
                  onPush((op) => {
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
                      queueMicrotask(() => {
                        remoteResults$.next(buildResult(op));
                        const teardown = subscriptions.get(txId);
                        subscriptions.delete(txId);
                        if (teardown) setTimeout(teardown, 0);
                      });
                    } else pendingRemoteOps.set(txId, op);
                  }),
                ),
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
                      variables: op.variables ?? {},
                    });
                  } catch (err) {
                    outgoingMutations.delete(correlationKey(op));
                    console.warn(
                      "[urql-crosstab-sync] failed to broadcast mutation:start",
                      err,
                    );
                  }
                }),
              ),
            ),
            onPush((result) => {
              const op = result.operation;
              if (getCrossTabMeta(op)?.remote) return;
              if (op.kind === "mutation") {
                if (!syncMutations) return;
                const cKey = correlationKey(op);
                const txId = outgoingMutations.get(cKey);
                if (!txId) return;
                outgoingMutations.delete(cKey);
                try {
                  if (result.error)
                    channel.postMessage({
                      type: "mutation:error",
                      tabId,
                      txId,
                      error: serializeError(result.error),
                    });
                  else
                    channel.postMessage({
                      type: "mutation:result",
                      tabId,
                      txId,
                      data: result.data,
                      extensions: result.extensions,
                    });
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
                    variables: op.variables ?? {},
                    data: result.data,
                    extensions: result.extensions,
                  });
                } catch (err) {
                  console.warn(
                    "[urql-crosstab-sync] failed to broadcast query:result",
                    err,
                  );
                }
              }
            }),
          ),
          remoteResults$.source,
        ]),
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

//#endregion
export { crossTabSyncExchange };

//# sourceMappingURL=index.mjs.map
