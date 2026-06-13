/**
 * Debug logger surface. Set `debug: true` on the exchange to log to
 * `console.debug`, or pass a function to capture events in your own sink.
 */
export type Logger = (event: string, data?: unknown) => void;
export type DebugOption = boolean | Logger;

const NOOP: Logger = () => {};

/**
 * Build a Logger. The returned logger is a no-op when `debug` is falsy, your
 * function when `debug` is a function, or `console.debug` otherwise. The
 * short prefix `[xts <tabId-prefix>]` makes it easy to tell tabs apart in
 * a shared console.
 */
export function createLogger(
  debug: DebugOption | undefined,
  tabId: string,
): Logger {
  if (!debug) return NOOP;
  if (typeof debug === "function") {
    return (event, data) => {
      debug(event, data === undefined ? { tabId } : { tabId, ...wrap(data) });
    };
  }
  const prefix = `[xts ${tabId.slice(0, 8)}]`;
  return (event, data) => {
    if (data !== undefined) {
      console.debug(`${prefix} ${event}`, data);
    } else {
      console.debug(`${prefix} ${event}`);
    }
  };
}

function wrap(data: unknown): Record<string, unknown> {
  if (data && typeof data === "object" && !Array.isArray(data)) {
    return data as Record<string, unknown>;
  }
  return { data };
}
