# kalam_link — Developer Notes

This document is for contributors and developers working on the `kalam_link` Dart SDK itself.
For usage documentation, see [README.md](README.md).

## Architecture

The Dart SDK is a thin Dart layer over the shared Rust client, bridged via
[flutter_rust_bridge](https://cjycode.com/flutter_rust_bridge/) v2.

```
Flutter App
  └─ kalam_link (Dart package)         ← this package
      └─ Generated FRB bindings          ← lib/src/generated/ (auto-generated, do not edit)
          └─ kalam-link-dart (Rust)      ← link/kalam-link-dart/  (bridge crate)
              └─ kalam-client            ← link/kalam-client/  (Rust/WASM entry crate)
                  └─ link-common         ← link/link-common/src/  (shared client implementation)
```

The FRB codegen tool reads `#[frb]`-annotated Rust functions in `kalam-link-dart` and generates
`lib/src/generated/frb_generated.dart` (+ IO and Web variants). All FFI, async dispatch, and type
marshalling is handled automatically.

## Prerequisites

| Tool | Version |
|------|---------|
| Flutter SDK | >= 3.10 |
| Dart SDK | >= 3.3.0 |
| Rust toolchain | stable (via `rustup`) |
| `flutter_rust_bridge_codegen` | v2.x |
| Android NDK (for Android builds) | as required by Flutter |

Install the FRB codegen tool:

```bash
dart pub global activate flutter_rust_bridge
```

## Generating Dart Bindings

Run this whenever you change Rust-side API in `link/kalam-link-dart`:

```bash
cd link/kalam-link-dart
flutter_rust_bridge_codegen generate
```

Generated files land in `link/sdks/dart/lib/src/generated/` — commit them together with the
Rust changes.

## Local Scripts

From `link/sdks/dart`:

```bash
./build.sh                                # canonical build: deps + optional FRB generation + native artefacts + analyze
./build.sh all                            # build every platform (requires all cross-compile toolchains)
./build.sh android ios web                # build specific platforms and prepare the SDK
./build.sh android --all-abis             # Android all four ABIs + SDK prep
./test.sh                                 # flutter pub get + dart analyze + flutter test
./publish.sh                              # validate platform artefacts + publish to pub.dev
./publish.sh --dry-run                    # validate only, no upload
```

Control FRB regeneration:

```bash
FRB_GENERATE=always ./build.sh   # always regenerate
FRB_GENERATE=never  ./build.sh   # skip regeneration
```

Choose the platform set explicitly when needed:

```bash
./build.sh                              # auto-detect platforms for this host
BUILD_PLATFORMS="android ios" ./build.sh # env override if you prefer not to pass args
```

## Platform Support

KalamDB's Flutter plugin ships **pre-built native libraries** for every supported platform
so consuming apps do not need Rust or cross-compilation toolchains.

### Supported platforms

| Platform | Artefact | Location |
|----------|----------|----------|
| Android arm64-v8a | `.so` (shared) | `android/src/main/jniLibs/arm64-v8a/` |
| Android x86_64 | `.so` (shared) | `android/src/main/jniLibs/x86_64/` |
| iOS (aarch64) | `.a` (static) | `ios/Frameworks/` |
| macOS (universal) | `.dylib` (shared) | `macos/Libs/` |
| Linux x86_64 | `.so` (shared) | `linux/lib/` |
| Windows x86_64 | `.dll` (shared) | `windows/lib/` |
| Web (WASM) | `.wasm` | `web/pkg/` |

### Why the native files need to be committed

The root `.gitignore` ignores `*.so`, `*.dylib`, and `*.wasm` build artefacts. Each platform
directory has its own `.gitignore` override (e.g. `!*.so`) to re-include the pre-built
libraries. **Always commit the native files** before publishing a new version.

### Building native libraries

Prerequisites vary by platform:

| Platform | Prerequisites |
|----------|--------------|
| Android | Rust stable, `cargo-ndk`, Android NDK r25c+ |
| iOS | Rust stable, Xcode, `rustup target add aarch64-apple-ios aarch64-apple-ios-sim` |
| macOS | Rust stable, Xcode, `rustup target add aarch64-apple-darwin x86_64-apple-darwin` |
| Linux | Rust stable, `rustup target add x86_64-unknown-linux-gnu` |
| Windows | Rust stable, `rustup target add x86_64-pc-windows-gnu`, MinGW toolchain |
| Web | Rust stable, `wasm-pack`, `rustup target add wasm32-unknown-unknown` |

```bash
# From link/sdks/dart

# Auto-detect and build for current OS platforms
./build.sh

# Build for specific platforms
./build.sh android ios macos web

# Build everything (requires all cross-compile toolchains)
./build.sh all

# Commit and prepare for publish
git add android/ ios/ macos/ linux/ windows/ web/
git commit -m "chore(dart): rebuild native libs for all platforms"
```

## Running Tests

### Unit tests

```bash
flutter test
```

### Live server integration tests

Requires a running KalamDB instance (default: `http://localhost:8080`, user `admin`, pass `kalamdb123`):

```bash
KALAM_INTEGRATION_TEST=1 \
KALAM_URL=http://localhost:8080 \
KALAM_USER=admin \
KALAM_PASS=kalamdb123 \
flutter test test/live_server_test.dart
```

Notes:
- Integration tests create and drop temporary tables — safe to run against a dev instance.
- Set `KALAM_BUILD_DART_BRIDGE=1` (default) to auto-build the Rust bridge before tests if needed.

## Publishing

```bash
cd link/sdks/dart
./publish.sh            # full publish
./publish.sh --dry-run  # validate only
```

The script checks for a clean git state, runs `dart analyze`, optionally runs tests, then publishes
to [pub.dev](https://pub.dev/packages/kalam_link).

Before a new release:
1. Bump `version` in `pubspec.yaml`
2. Add an entry to `CHANGELOG.md`
3. Rebuild native libs: `./build.sh all` (commit all artefacts)
4. Commit + tag (`git tag dart-v0.x.y`)
5. Run `./publish.sh` (validates all platform artefacts exist before uploading)

## Crate Layout

| Path | Purpose |
|------|---------|
| `link/kalam-client/` | Rust/WASM entry crate used by the Dart bridge |
| `link/link-common/src/` | Shared Rust client implementation (HTTP, WebSocket, auth) |
| `link/kalam-link-dart/` | FRB bridge crate — wraps the shared client with `#[frb]` annotations |
| `link/sdks/dart/lib/src/generated/` | Auto-generated Dart bindings (do not edit manually) |
| `link/sdks/dart/lib/src/` | Hand-written Dart API layer (`kalam_client.dart`, `auth.dart`, `models.dart`) |
