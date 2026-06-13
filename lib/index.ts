import type { Exchange, Operation } from "@urql/core";
import { filter, merge, onEnd, onPush, pipe } from "wonka";

import { createBroadcaster } from "./broadcaster.ts";
import { generateId, isBrowser } from "./env.ts";
import { getCrossTabMeta } from "./operation.ts";
import { isMessage } from "./protocol.ts";
import { createReceiver } from "./receiver.ts";

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

/**
 * Place this exchange below graphcache and above the network exchange
 * (`fetchExchange`, or `subscriptionExchange` in WS-only setups). It
 * mirrors mutations and successful query results between same-origin tabs
 * over `BroadcastChannel`.
 */
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
    const receiver = createReceiver({ client, syncMutations, syncQueries });
    const broadcaster = createBroadcaster({
      channel,
      tabId,
      shouldSync,
      syncMutations,
      syncQueries,
    });

    channel.addEventListener("message", (event: MessageEvent) => {
      const msg = event.data;
      if (!isMessage(msg) || msg.tabId === tabId) return;
      receiver.handleMessage(msg);
    });

    return (ops$) => {
      const observed$ = pipe(
        ops$,
        onPush((op) => receiver.observe(op)),
      );

      // Short-circuit remote-marked ops away from the next exchange. Teardowns
      // must still pass through so downstream exchanges clean up their state.
      const forwardable$ = pipe(
        observed$,
        filter((op) => op.kind === "teardown" || !getCrossTabMeta(op)?.remote),
        onPush((op) => broadcaster.onOutgoingOperation(op)),
      );

      const forwarded$ = pipe(
        forward(forwardable$),
        onPush((result) => broadcaster.onIncomingResult(result)),
      );

      return pipe(
        merge([forwarded$, receiver.results$]),
        onEnd(() => {
          channel.close();
          receiver.dispose();
        }),
      );
    };
  };
}
