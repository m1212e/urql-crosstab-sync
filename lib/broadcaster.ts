import type { Operation, OperationResult } from "@urql/core";
import { stringifyDocument } from "@urql/core";

import { generateId } from "./env.ts";
import type { Logger } from "./logger.ts";
import { correlationKey, getCrossTabMeta } from "./operation.ts";
import type { Message } from "./protocol.ts";
import { serializeError } from "./protocol.ts";

/**
 * The broadcaster owns the outbound half: it observes locally-initiated
 * operations and their results, and posts the corresponding messages on the
 * shared `BroadcastChannel`. It does not produce any side effects on the
 * exchange stream.
 */
export interface Broadcaster {
  /** Called when an operation has been observed flowing toward the network. */
  onOutgoingOperation(op: Operation): void;
  /** Called when a result has been observed flowing back from the network. */
  onIncomingResult(result: OperationResult): void;
}

export interface BroadcasterConfig {
  channel: BroadcastChannel;
  tabId: string;
  shouldSync: (op: Operation) => boolean;
  syncMutations: boolean;
  syncQueries: boolean;
  log: Logger;
}

export function createBroadcaster({
  channel,
  tabId,
  shouldSync,
  syncMutations,
  syncQueries,
  log,
}: BroadcasterConfig): Broadcaster {
  /** correlationKey(op) → txId for locally-initiated mutations. */
  const outgoing = new Map<string, string>();

  function post(message: Message): void {
    log("broadcaster:post", message);
    try {
      channel.postMessage(message);
    } catch (err) {
      log("broadcaster:post:failed", {
        type: message.type,
        error: String(err),
      });
      console.warn(
        `[urql-crosstab-sync] failed to broadcast ${message.type}`,
        err,
      );
    }
  }

  function onOutgoingOperation(op: Operation): void {
    if (op.kind === "teardown") return;
    if (op.kind !== "mutation") {
      log("broadcaster:outgoing:ignore", {
        reason: `kind=${op.kind} (only mutations broadcast on dispatch)`,
        opKey: op.key,
      });
      return;
    }
    if (!syncMutations) {
      log("broadcaster:outgoing:skip", {
        reason: "syncMutations=false",
        opKey: op.key,
      });
      return;
    }
    if (!shouldSync(op)) {
      log("broadcaster:outgoing:skip", {
        reason: "shouldSync returned false",
        opKey: op.key,
      });
      return;
    }
    if (getCrossTabMeta(op)?.remote) {
      log("broadcaster:outgoing:skip", {
        reason: "synthetic remote op",
        opKey: op.key,
      });
      return;
    }

    const txId = generateId();
    const cKey = correlationKey(op);
    outgoing.set(cKey, txId);
    log("broadcaster:outgoing:correlate", {
      txId,
      cKey,
      opKey: op.key,
      _instance: (op.context as { _instance?: unknown })._instance,
    });

    post({
      type: "mutation:start",
      tabId,
      txId,
      query: stringifyDocument(op.query),
      variables: (op.variables as Record<string, unknown> | undefined) ?? {},
    });
  }

  function onIncomingResult(result: OperationResult): void {
    const op = result.operation;
    if (getCrossTabMeta(op)?.remote) {
      log("broadcaster:incoming:skip", {
        reason: "remote-originated result",
        kind: op.kind,
        opKey: op.key,
      });
      return;
    }

    if (op.kind === "mutation") {
      if (!syncMutations) return;
      const cKey = correlationKey(op);
      const txId = outgoing.get(cKey);
      if (!txId) {
        log("broadcaster:incoming:no-correlation", {
          cKey,
          opKey: op.key,
          known: [...outgoing.keys()],
        });
        return;
      }
      outgoing.delete(cKey);

      if (result.error) {
        post({
          type: "mutation:error",
          tabId,
          txId,
          error: serializeError(result.error),
        });
      } else {
        post({
          type: "mutation:result",
          tabId,
          txId,
          data: result.data,
          extensions: result.extensions,
        });
      }
      return;
    }

    if (op.kind === "query") {
      if (!syncQueries) {
        log("broadcaster:incoming:skip", {
          reason: "syncQueries=false",
          opKey: op.key,
        });
        return;
      }
      if (result.error) {
        log("broadcaster:incoming:skip", {
          reason: "query result has error",
          opKey: op.key,
        });
        return;
      }
      if (!shouldSync(op)) {
        log("broadcaster:incoming:skip", {
          reason: "shouldSync returned false",
          opKey: op.key,
        });
        return;
      }
      post({
        type: "query:result",
        tabId,
        query: stringifyDocument(op.query),
        variables: (op.variables as Record<string, unknown> | undefined) ?? {},
        data: result.data,
        extensions: result.extensions,
      });
    }
  }

  return { onOutgoingOperation, onIncomingResult };
}
