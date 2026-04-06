# KalamDB SDK Workspace

Shared Rust transport crates and SDK packages for KalamDB clients.

This directory contains:

- the shared Rust implementation crate `link-common`
- the app-facing Rust/WASM entry crate `kalam-client`
- the worker-focused Rust crate `kalam-consumer`
- the consumer-only WebAssembly wrapper crate `kalam-consumer-wasm`
- the publishable TypeScript app-facing package in [sdks/typescript/client](sdks/typescript/client/README.md) as `@kalamdb/client`
- the publishable TypeScript worker package in [sdks/typescript/consumer](sdks/typescript/consumer/README.md) as `@kalamdb/consumer`
- the publishable Dart/Flutter package in [sdks/dart](sdks/dart/README.md) as `kalam_link`

## Canonical SDK Docs

Use the package-specific READMEs as the source of truth for public APIs:

- TypeScript / JavaScript app client: [sdks/typescript/client/README.md](sdks/typescript/client/README.md)
- TypeScript / JavaScript worker client: [sdks/typescript/consumer/README.md](sdks/typescript/consumer/README.md)
- Dart / Flutter SDK: [sdks/dart/README.md](sdks/dart/README.md)

Older constructor-based examples, manual `connect()` walkthroughs, and raw WASM `KalamClient(...)` snippets are not accurate for the current SDKs.

## Current SDK Shape

### TypeScript / JavaScript

- Use `@kalamdb/client` for `createClient({ url, authProvider, ... })`, SQL, auth flows, realtime subscriptions, live rows, and file helpers
- Use `@kalamdb/consumer` for topic workers: `consumer()`, `consumeBatch()`, `ack()`, `runAgent()`, and `runConsumer()`
- `authProvider` is required and can return `Auth.basic(...)`, `Auth.jwt(...)`, or `Auth.none()`
- WebSocket connection management is automatic; with `wsLazyConnect: true` the client connects on the first realtime call

### Dart / Flutter

- Call `KalamClient.init()` once before first use
- Create clients with `KalamClient.connect(url: ..., authProvider: ...)`
- Auth flows use `authProvider`, `login(...)`, `refreshToken(...)`, and `refreshAuth(...)`
- The SDK exposes low-level events via `subscribe(...)` and materialized row helpers via `liveQueryRowsWithSql()` / `liveTableRows()`
- Topic worker APIs are intentionally not part of the Dart SDK yet

## Repository Layout

```text
link/
|-- link-common/          # shared Rust implementation
|-- kalam-client/         # app-facing Rust/WASM entry crate
|-- kalam-consumer/       # worker-focused Rust crate
|-- kalam-consumer-wasm/  # wasm-bindgen wrapper for @kalamdb/consumer
`-- sdks/
    |-- typescript/
    |   |-- client/       # npm package: @kalamdb/client
    |   `-- consumer/     # npm package: @kalamdb/consumer
    `-- dart/             # pub package: kalam_link
```

## Build Notes

Package-specific build, test, and publish instructions live with each SDK:

- TypeScript / JavaScript app client: [sdks/typescript/client/README.md](sdks/typescript/client/README.md)
- TypeScript / JavaScript worker client: [sdks/typescript/consumer/README.md](sdks/typescript/consumer/README.md)
- Dart / Flutter: [sdks/dart/README.md](sdks/dart/README.md)

If you change the shared Rust implementation in `link-common` or the `kalam-client` WASM entry crate, validate the affected SDK package afterward.

## License

See the main KalamDB repository for license information.

## Contributing

See the main KalamDB repository for contribution guidelines.
