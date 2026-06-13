# @m1212e/urql-crosstab-sync

A [urql](https://urql.dev/) exchange that mirrors mutations and query results
between same-origin browser tabs over `BroadcastChannel`, so cache state stays
in sync — including optimistic updates from
[`@urql/exchange-graphcache`](https://github.com/urql-graphql/urql/tree/main/exchanges/graphcache).

When a user fires a mutation in tab A, every other tab plays the same mutation
through its own graphcache: optimistic update on dispatch, real data on
success, rollback on error. No extra wiring inside your graphcache config — it
just sees a mutation come through the pipeline.

## Install

```sh
bun add @m1212e/urql-crosstab-sync
# or
npm i @m1212e/urql-crosstab-sync
```

Peer deps: `@urql/core ^6`, `wonka ^6`.

## Setup

The exchange must sit **between `cacheExchange` (graphcache) and
`fetchExchange`**:

```ts
import { createClient, fetchExchange } from "@urql/core";
import { cacheExchange } from "@urql/exchange-graphcache";
import { crossTabSyncExchange } from "@m1212e/urql-crosstab-sync";

const client = createClient({
  url: "/graphql",
  exchanges: [
    cacheExchange({
      /* your graphcache config: optimistic, updates, resolvers, ... */
    }),
    crossTabSyncExchange(),
    fetchExchange,
  ],
});
```

That placement matters: the sync exchange relies on graphcache having already
applied its optimistic phase on outbound mutations, and it needs to be able to
short-circuit the network for synthetic operations triggered by other tabs.

## What gets synced

| Event in tab A                  | What tab B does                                                |
| ------------------------------- | -------------------------------------------------------------- |
| Mutation starts                 | Dispatches the same mutation — graphcache runs `optimistic`    |
| Mutation succeeds               | Resolves tab B's synthetic mutation with the real result       |
| Mutation errors                 | Resolves tab B's mutation with the same error → graphcache rolls back |
| Query result returns from network | Writes the same data into tab B's cache (synthetic query)    |

Subscriptions and teardowns are forwarded normally and never broadcast.

## Options

```ts
crossTabSyncExchange({
  channelName: "urql-crosstab-sync", // BroadcastChannel name
  tabId: undefined,                  // defaults to crypto.randomUUID()
  shouldSync: (op) => true,          // predicate to opt operations out of sync
  syncMutations: true,
  syncQueries: true,
});
```

`shouldSync` is the escape hatch you want for things like login/logout
mutations:

```ts
crossTabSyncExchange({
  shouldSync: (op) => {
    const name = op.query.definitions[0];
    if (name?.kind === "OperationDefinition" && name.name?.value === "Login") {
      return false;
    }
    return true;
  },
});
```

Outside the browser (SSR, Node), the exchange becomes a passthrough — safe to
include in isomorphic client setups.

## Caveats

- Variables and result data must be **structured-cloneable**
  (`BroadcastChannel.postMessage` uses the structured clone algorithm). `Date`,
  `Map`, `Set`, typed arrays, `File`, and `Blob` work; closures, class
  instances with private fields, and DOM nodes don't. A failed `postMessage`
  is caught and logged via `console.warn`; it won't break the originating
  tab's mutation.
- The protocol assumes all tabs are running the **same schema and the same
  graphcache config**. If two tabs disagree on `optimistic` or `updates`
  handlers, their caches will drift.
- If two tabs fire the same mutation concurrently, each tab will execute its
  own original mutation and a synthetic replay of the other tab's — the final
  cache state is correct but you'll see two server round-trips. Use
  `shouldSync` to opt out for mutations where that's undesirable.
- If tab A closes before its mutation result arrives, tab B's optimistic
  update will not be rolled back automatically. This may be addressed in a
  later version with a pending-mutation timeout.

## License

Apache-2.0
