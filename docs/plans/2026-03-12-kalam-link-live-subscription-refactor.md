# Kalam Link Live Subscription Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move subscription resume, replay filtering, snapshot batching, and live-row materialization ownership into `kalam-client` / `link-common` so the TypeScript and Dart SDKs become thin bindings over one stable core.

**Architecture:** Keep the shared connection and WASM connection models, but extract the duplicated checkpoint/replay rules into shared Rust helpers used by native `SubscriptionManager`, native `SharedConnection`, and WASM subscription state. SDK wrappers should stop re-implementing reconnect or replay filtering and instead translate Rust events into language-native streams/callbacks. Verification must focus on no missed events, no replay across reconnects, and deterministic teardown behavior.

**Tech Stack:** Rust 2021, tokio, tokio-tungstenite, wasm-bindgen, flutter_rust_bridge, TypeScript, Dart, Node test runner, Flutter test

---

### Task 1: Freeze Current Behavior And Safety Rails

**Files:**
- Modify: `link/kalam-client/tests/proxied.rs`
- Modify: `link/kalam-client/tests/proxied/helpers.rs`
- Create/Adjust: `link/kalam-client/tests/proxied/update_delete_resume.rs`
- Check: `link/sdks/typescript/client/tests/e2e/reconnect/resume.test.mjs`
- Check: `link/sdks/dart/test/e2e/reconnect/resume_test.dart`

**Step 1: Confirm the required guarantees**

Document and preserve these invariants while refactoring:
- reconnect resumes from the highest consumed `_seq`
- replayed rows at the `from` boundary are filtered exactly once
- initial snapshot batches finish before buffered live changes are released
- update/delete resumes do not replay pre-disconnect rows
- explicit unsubscribe/dispose stops delivery without hanging `next()`

**Step 2: Add or tighten tests for the invariants that are not covered at the Rust boundary**

Prefer targeted tests around:
- resumed `initial_data_batch` filtering
- update/delete replay suppression
- large snapshot + reconnect buffering order
- live-row materialization across reconnect

**Step 3: Run the targeted failing tests first**

Run:
```bash
cd /Users/jamal/git/KalamDB && cargo nextest run -p kalam-client proxied:: --failure-output immediate > /tmp/kalam_client_proxied_before.txt 2>&1
```

Expected: current failures or existing pass state captured before refactor.

### Task 2: Extract Native Subscription Resume Core

**Files:**
- Create: `link/link-common/src/subscription/checkpoint.rs`
- Modify: `link/link-common/src/subscription/mod.rs`
- Modify: `link/link-common/src/subscription/manager.rs`
- Modify: `link/link-common/src/seq_tracking.rs`

**Step 1: Write the shared checkpoint/replay API**

Move duplicated logic out of `manager.rs` into a focused helper that owns:
- replay filtering for `InitialDataBatch`, `Insert`, `Update`, `Delete`
- checkpoint extraction from rows and batch control
- buffering transition rules for loading vs ready

**Step 2: Update `SubscriptionManager` to use the helper**

Keep `SubscriptionManager` responsible only for:
- draining the event queue
- reporting shared progress
- closing/unsubscribing

Remove inline replay/filter logic and make buffering state transitions explicit with small private methods.

**Step 3: Add unit tests around the new helper**

Cover:
- inclusive `from` filtering
- empty filtered update/delete suppression
- ready-transition buffer flush ordering

### Task 3: Simplify Shared Native Connection State Machine

**Files:**
- Modify: `link/link-common/src/connection/shared.rs`

**Step 1: Collapse duplicated command handling**

Extract helpers for:
- inserting/replacing subscription entries
- caching final seq for closed subscriptions
- applying progress updates
- removing entries with generation checks

**Step 2: Reuse the shared checkpoint/replay helper**

Replace local `filter_replayed_rows` and scattered `_seq` updates with the shared logic from Task 2.

**Step 3: Make reconnect behavior explicit**

Keep one source of truth for:
- inherited cached seq on resubscribe
- `pending_result_tx` completion
- resubscribe `from` selection
- keepalive failure transition to reconnect

### Task 4: Move WASM Resume And Live Core Fully Into Rust

**Files:**
- Modify: `link/link-common/src/wasm/state.rs`
- Modify: `link/link-common/src/wasm/client.rs`
- Modify: `link/link-common/src/wasm/reconnect.rs`
- Modify: `link/link-common/src/subscription/live_rows_materializer.rs`

**Step 1: Add replay/checkpoint helpers usable by WASM subscription state**

WASM should track:
- requested resume cursor
- last delivered cursor
- loading/buffer state if needed for ordered release

**Step 2: Filter replayed rows in WASM before JavaScript callbacks**

This removes the need for TypeScript-side `filterSubscriptionEventAfterCheckpoint`.

**Step 3: Keep live-row materialization in Rust**

Ensure `liveQueryRowsWithSql` remains the primary path and that callback payloads are produced from Rust materializers, not reconstructed in TypeScript.

### Task 5: Remove SDK-Level Reconnect/Replay Duplication

**Files:**
- Modify: `link/sdks/typescript/client/src/client.ts`
- Modify: `link/sdks/dart/lib/src/kalam_client.dart`
- Modify: `link/kalam-link-dart/src/api.rs`

**Step 1: TypeScript**

Remove:
- replay filtering helpers that duplicate Rust behavior
- low-value bookkeeping that only mirrors WASM state

Keep:
- argument normalization
- language-friendly callback typing
- optional fallback only where Rust cannot express the API contract

**Step 2: Dart**

Replace the manual reconnect loop with a single subscription task backed by Rust’s shared connection lifecycle.

Keep only:
- auth refresh before initial subscribe if required
- stream-controller glue
- cancellation/disposal plumbing

**Step 3: Verify FRB bridge expectations**

Ensure `dart_subscription_next` / `dart_live_query_rows_next` still unblock on close/cancel and do not require the Dart layer to reopen subscriptions.

### Task 6: Verify Stability And Regression Safety

**Files:**
- Check: `link/kalam-client/src/lib.rs`
- Check: `link/link-common/src/connection/shared.rs`
- Check: `link/link-common/src/subscription/manager.rs`
- Check: `link/link-common/src/wasm/client.rs`
- Check: `link/sdks/typescript/client/src/client.ts`
- Check: `link/sdks/dart/lib/src/kalam_client.dart`

**Step 1: Run one batched Rust compile**

Run:
```bash
cd /Users/jamal/git/KalamDB && cargo check -p kalam-client -p kalam-consumer -p kalam-consumer-wasm -p kalam-link-dart > /tmp/kalam_client_check.txt 2>&1
```

Fix all reported issues before re-running.

**Step 2: Run focused Rust tests**

Run:
```bash
cd /Users/jamal/git/KalamDB && cargo nextest run -p kalam-client proxied:: socket_drop_resume live_updates_resume update_delete_resume > /tmp/kalam_client_resume_tests.txt 2>&1
```

**Step 3: Run SDK reconnect suites**

Run:
```bash
cd /Users/jamal/git/KalamDB/link/sdks/typescript/client && node --test --test-concurrency=1 tests/e2e/reconnect/resume.test.mjs
cd /Users/jamal/git/KalamDB/link/sdks/dart && flutter test test/e2e/reconnect/resume_test.dart
```

**Step 4: Run broader subscription smoke if the server is available**

Run:
```bash
cd /Users/jamal/git/KalamDB/cli && cargo test --test smoke subscription -- --nocapture
```

**Step 5: Review final behavior**

Confirm:
- no SDK-layer replay filtering remains where Rust already guarantees it
- no Dart reconnect loop remains for shared subscriptions
- subscription info still reports stable `last_seq_id`
- manual disconnect/reconnect paths still behave deterministically
