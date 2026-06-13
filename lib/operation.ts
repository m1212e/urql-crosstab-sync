import type { Operation } from "@urql/core";

/**
 * Marker attached to the `OperationContext` of a synthetic operation that
 * this exchange dispatched in response to a remote tab's broadcast. The
 * exchange uses this to short-circuit the network for these operations and
 * to route remote results back to them.
 */
export interface CrossTabMeta {
  remote: true;
  txId: string;
}

export function getCrossTabMeta(
  operation: Operation,
): CrossTabMeta | undefined {
  return (operation.context as { crossTabSync?: CrossTabMeta }).crossTabSync;
}

/**
 * Correlation key for matching a mutation's outgoing operation to its
 * incoming result. `_instance` distinguishes concurrent invocations of the
 * same mutation; for queries it falls back to the deterministic operation key.
 */
export function correlationKey(op: Operation): string {
  const inst = (op.context as { _instance?: unknown })._instance;
  return inst !== undefined ? `i:${String(inst)}` : `k:${op.key}`;
}
