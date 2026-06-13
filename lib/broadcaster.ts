import type { Operation, OperationResult } from "@urql/core";
import { stringifyDocument } from "@urql/core";

import { generateId } from "./env.ts";
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
}

export function createBroadcaster({
  channel,
  tabId,
  shouldSync,
  syncMutations,
  syncQueries,
}: BroadcasterConfig): Broadcaster {
  /** correlationKey(op) → txId for locally-initiated mutations. */
  const outgoing = new Map<string, string>();

  function post(message: Message, context: string): void {
    try {
      channel.postMessage(message);
    } catch (err) {
      console.warn(`[urql-crosstab-sync] failed to broadcast ${context}`, err);
    }
  }

  function onOutgoingOperation(op: Operation): void {
    if (op.kind !== "mutation") return;
    if (!syncMutations) return;
    if (!shouldSync(op)) return;
    if (getCrossTabMeta(op)?.remote) return; // synthetic remote ops aren't re-broadcast

    const txId = generateId();
    const cKey = correlationKey(op);
    outgoing.set(cKey, txId);

    post(
      {
        type: "mutation:start",
        tabId,
        txId,
        query: stringifyDocument(op.query),
        variables: (op.variables as Record<string, unknown> | undefined) ?? {},
      },
      "mutation:start",
    );
  }

  function onIncomingResult(result: OperationResult): void {
    const op = result.operation;
    if (getCrossTabMeta(op)?.remote) return; // remote-originated results never echo

    if (op.kind === "mutation") {
      if (!syncMutations) return;
      const cKey = correlationKey(op);
      const txId = outgoing.get(cKey);
      if (!txId) return;
      outgoing.delete(cKey);

      if (result.error) {
        post(
          {
            type: "mutation:error",
            tabId,
            txId,
            error: serializeError(result.error),
          },
          "mutation:error",
        );
      } else {
        post(
          {
            type: "mutation:result",
            tabId,
            txId,
            data: result.data,
            extensions: result.extensions,
          },
          "mutation:result",
        );
      }
      return;
    }

    if (op.kind === "query") {
      if (!syncQueries) return;
      if (result.error) return;
      if (!shouldSync(op)) return;
      post(
        {
          type: "query:result",
          tabId,
          query: stringifyDocument(op.query),
          variables:
            (op.variables as Record<string, unknown> | undefined) ?? {},
          data: result.data,
          extensions: result.extensions,
        },
        "query:result",
      );
    }
  }

  return { onOutgoingOperation, onIncomingResult };
}
