/** Generates a short, opaque, unique identifier (UUID when available). */
export function generateId(): string {
  const c =
    typeof globalThis !== "undefined"
      ? (globalThis as { crypto?: { randomUUID?: () => string } }).crypto
      : undefined;
  if (c?.randomUUID) return c.randomUUID();
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}

/** True when running in an environment with `BroadcastChannel` available. */
export function isBrowser(): boolean {
  return (
    typeof globalThis !== "undefined" &&
    typeof (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel !==
      "undefined"
  );
}
