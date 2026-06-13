import { Exchange, Operation } from "@urql/core";

//#region lib/index.d.ts
interface CrossTabSyncOptions {
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
declare function crossTabSyncExchange(options?: CrossTabSyncOptions): Exchange;

//#endregion
export { CrossTabSyncOptions, crossTabSyncExchange };
//# sourceMappingURL=index.d.mts.map
