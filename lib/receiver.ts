import type { Client, Operation, OperationResult } from "@urql/core";
import { createRequest } from "@urql/core";
import type { Source } from "wonka";
import { makeSubject, pipe, subscribe } from "wonka";

import { generateId } from "./env.ts";
import type { Logger } from "./logger.ts";
import { getCrossTabMeta } from "./operation.ts";
import type { Message } from "./protocol.ts";
import { deserializeError } from "./protocol.ts";

/**
 * The receiver owns the inbound half: it listens to remote tab messages,
 * dispatches synthetic urql operations so graphcache plays through its full
 * lifecycle (optimistic → real result), and emits synthesized
 * `OperationResult`s on its `results$` source for the exchange to merge into
 * its output.
 */
export interface Receiver {
  /** Stream of synthesized `OperationResult`s to merge into the exchange output. */
  results$: Source<OperationResult>;
  /** Process a message broadcast by another tab. */
  handleMessage(msg: Message): void;
  /**
   * Notify the receiver that an operation has flowed through the exchange.
   * For synthetic remote-flagged operations this either delivers a stashed
   * early result, parks the operation pending a result message, or cleans
   * up on teardown.
   */
  observe(op: Operation): void;
  /** Tear down all internal state. */
  dispose(): void;
}

export interface ReceiverConfig {
  client: Client;
  syncMutations: boolean;
  syncQueries: boolean;
  log: Logger;
}

type ResultBuilder = (op: Operation) => OperationResult;

export function createReceiver({
  client,
  syncMutations,
  syncQueries,
  log,
}: ReceiverConfig): Receiver {
  const subject = makeSubject<OperationResult>();
  /** Synthetic remote ops awaiting a result message. */
  const pendingOps = new Map<string, Operation>();
  /** Results received before their synthetic op was observed in our stream. */
  const earlyResults = new Map<string, ResultBuilder>();
  /** Subscription teardowns for synthetic ops, kept alive until result emission. */
  const subscriptions = new Map<string, () => void>();

  function deliver(txId: string, build: ResultBuilder): void {
    const op = pendingOps.get(txId);
    if (op) {
      pendingOps.delete(txId);
      log("receiver:result:emit", { txId, opKey: op.key, kind: op.kind });
      subject.next(build(op));
      tearDown(txId);
    } else {
      log("receiver:result:park", {
        txId,
        reason: "synthetic op not yet observed",
      });
      earlyResults.set(txId, build);
    }
  }

  /** Deferred so the result propagates fully before unsubscribing. */
  function tearDown(txId: string): void {
    const teardown = subscriptions.get(txId);
    subscriptions.delete(txId);
    if (teardown) {
      log("receiver:sub:teardown:schedule", { txId });
      setTimeout(() => {
        log("receiver:sub:teardown:run", { txId });
        teardown();
      }, 0);
    }
  }

  function dispatchSynthetic(
    kind: "mutation" | "query",
    query: string,
    variables: Record<string, unknown>,
    txId: string,
  ): void {
    let req: ReturnType<typeof createRequest>;
    try {
      req = createRequest(query, variables);
    } catch (err) {
      log("receiver:dispatch:parse-failed", { txId, kind, error: String(err) });
      return;
    }
    const op = client.createRequestOperation(kind, req, {
      crossTabSync: { remote: true, txId },
      ...(kind === "query" ? { requestPolicy: "network-only" as const } : {}),
    });
    log("receiver:dispatch", {
      txId,
      kind,
      opKey: op.key,
      requestPolicy: op.context.requestPolicy,
    });
    const sub = pipe(
      client.executeRequestOperation(op),
      // No-op consumer; keeps the operation "active" so the synthesized
      // result is routed to graphcache and any real subscribers.
      subscribe((result) => {
        log("receiver:dispatch:result-seen", {
          txId,
          opKey: result.operation.key,
          hasError: !!result.error,
          hasData: result.data !== undefined,
        });
      }),
    );
    subscriptions.set(txId, () => sub.unsubscribe());
  }

  function handleMessage(msg: Message): void {
    log("receiver:message", msg);
    switch (msg.type) {
      case "mutation:start": {
        if (!syncMutations) {
          log("receiver:skip", {
            reason: "syncMutations=false",
            txId: msg.txId,
          });
          return;
        }
        dispatchSynthetic("mutation", msg.query, msg.variables, msg.txId);
        return;
      }
      case "mutation:result": {
        if (!syncMutations) {
          log("receiver:skip", {
            reason: "syncMutations=false",
            txId: msg.txId,
          });
          return;
        }
        deliver(msg.txId, (op) => ({
          operation: op,
          data: msg.data,
          error: undefined,
          extensions: msg.extensions,
          stale: false,
          hasNext: false,
        }));
        return;
      }
      case "mutation:error": {
        if (!syncMutations) {
          log("receiver:skip", {
            reason: "syncMutations=false",
            txId: msg.txId,
          });
          return;
        }
        deliver(msg.txId, (op) => ({
          operation: op,
          data: undefined,
          error: deserializeError(msg.error),
          extensions: undefined,
          stale: false,
          hasNext: false,
        }));
        return;
      }
      case "query:result": {
        if (!syncQueries) {
          log("receiver:skip", { reason: "syncQueries=false" });
          return;
        }
        const txId = generateId();
        log("receiver:query:assign-tx", { txId });
        // Stash the builder first; it'll be applied when the synthetic op
        // is observed flowing through the exchange.
        earlyResults.set(txId, (op) => ({
          operation: op,
          data: msg.data,
          error: undefined,
          extensions: msg.extensions,
          stale: false,
          hasNext: false,
        }));
        dispatchSynthetic("query", msg.query, msg.variables, txId);
        return;
      }
    }
  }

  function observe(op: Operation): void {
    const meta = getCrossTabMeta(op);
    if (!meta?.remote) return;
    const { txId } = meta;

    if (op.kind === "teardown") {
      log("receiver:observe:teardown", { txId, opKey: op.key });
      pendingOps.delete(txId);
      earlyResults.delete(txId);
      const teardown = subscriptions.get(txId);
      subscriptions.delete(txId);
      if (teardown) teardown();
      return;
    }

    const build = earlyResults.get(txId);
    if (build) {
      log("receiver:observe:apply-early-result", {
        txId,
        opKey: op.key,
        kind: op.kind,
      });
      earlyResults.delete(txId);
      // Defer so graphcache finishes processing the op (e.g. running its
      // optimistic config) before the synthesized result arrives.
      queueMicrotask(() => {
        log("receiver:result:emit", { txId, opKey: op.key, kind: op.kind });
        subject.next(build(op));
        tearDown(txId);
      });
    } else {
      log("receiver:observe:park-op", {
        txId,
        opKey: op.key,
        kind: op.kind,
      });
      pendingOps.set(txId, op);
    }
  }

  function dispose(): void {
    log("receiver:dispose", {
      pendingOps: pendingOps.size,
      earlyResults: earlyResults.size,
      subscriptions: subscriptions.size,
    });
    pendingOps.clear();
    earlyResults.clear();
    for (const teardown of subscriptions.values()) teardown();
    subscriptions.clear();
  }

  return {
    results$: subject.source,
    handleMessage,
    observe,
    dispose,
  };
}
