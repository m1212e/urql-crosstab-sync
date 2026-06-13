import type { Exchange, Operation } from "@urql/core";
import { filter, merge, onEnd, onPush, pipe } from "wonka";

import { createBroadcaster } from "./broadcaster.ts";
import { generateId, isBrowser } from "./env.ts";
import type { DebugOption } from "./logger.ts";
import { createLogger } from "./logger.ts";
import { getCrossTabMeta } from "./operation.ts";
import { isMessage } from "./protocol.ts";
import { createReceiver } from "./receiver.ts";

export type { DebugOption, Logger } from "./logger.ts";

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
  /**
   * Enable debug logging. `true` logs every relevant event to `console.debug`,
   * a function receives `(event, data)` so you can route into your own sink.
   * Default: off.
   */
  debug?: DebugOption;
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
    debug,
  } = options;

  const log = createLogger(debug, tabId);

  if (!isBrowser()) {
    log("init:passthrough", { reason: "no BroadcastChannel" });
    return ({ forward }) =>
      (ops$) =>
        forward(ops$);
  }

  log("init", {
    tabId,
    channelName,
    syncMutations,
    syncQueries,
    hasShouldSync: shouldSync !== undefined,
  });

  return ({ client, forward }) => {
    const channel = new BroadcastChannel(channelName);
    const receiver = createReceiver({
      client,
      syncMutations,
      syncQueries,
      log,
    });
    const broadcaster = createBroadcaster({
      channel,
      tabId,
      shouldSync,
      syncMutations,
      syncQueries,
      log,
    });

    channel.addEventListener("message", (event: MessageEvent) => {
      const msg = event.data;
      if (!isMessage(msg)) {
        log("channel:message:invalid", { received: msg });
        return;
      }
      if (msg.tabId === tabId) {
        log("channel:message:self-echo", {
          type: msg.type,
          txId: "txId" in msg ? msg.txId : undefined,
        });
        return;
      }
      log("channel:message:in", {
        type: msg.type,
        from: msg.tabId.slice(0, 8),
      });
      receiver.handleMessage(msg);
    });

    return (ops$) => {
      const observed$ = pipe(
        ops$,
        onPush((op) => {
          log("stream:op", {
            kind: op.kind,
            opKey: op.key,
            remote: !!getCrossTabMeta(op)?.remote,
          });
          receiver.observe(op);
        }),
      );

      // Short-circuit remote-marked ops away from the next exchange. Teardowns
      // must still pass through so downstream exchanges clean up their state.
      const forwardable$ = pipe(
        observed$,
        filter((op) => {
          const meta = getCrossTabMeta(op);
          if (op.kind === "teardown") return true;
          if (meta?.remote) {
            log("stream:short-circuit", { opKey: op.key, txId: meta.txId });
            return false;
          }
          return true;
        }),
        onPush((op) => broadcaster.onOutgoingOperation(op)),
      );

      const forwarded$ = pipe(
        forward(forwardable$),
        onPush((result) => {
          log("stream:result", {
            kind: result.operation.kind,
            opKey: result.operation.key,
            hasError: !!result.error,
            remote: !!getCrossTabMeta(result.operation)?.remote,
          });
          broadcaster.onIncomingResult(result);
        }),
      );

      return pipe(
        merge([forwarded$, receiver.results$]),
        onEnd(() => {
          log("stream:end");
          channel.close();
          receiver.dispose();
        }),
      );
    };
  };
}
