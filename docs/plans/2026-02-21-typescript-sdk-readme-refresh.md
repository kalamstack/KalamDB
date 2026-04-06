# TypeScript SDK README Refresh Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite `link/sdks/typescript/client/README.md` to accurately document the current `@kalamdb/client` SDK API, including the split between app-facing client APIs and the worker-facing `@kalamdb/consumer` package.

**Architecture:** Derive content directly from `src/index.ts`, `src/client.ts`, `src/types.ts`, and `src/file_ref.ts`. Provide practical examples first, then a complete API reference section grouped by lifecycle/auth/query/subscription/topic/file helpers.

**Tech Stack:** Markdown README, TypeScript SDK source, npm package metadata

---

### Task 1: Build source-accurate README outline

**Files:**
- Modify: `/Users/jamal/git/KalamDB/link/sdks/typescript/client/README.md`

**Step 1: Capture export surface from `src/index.ts`**
- Ensure every documented symbol is actually exported.

**Step 2: Capture class methods from `src/client.ts`**
- Include all public methods with accurate signatures and caveats.

**Step 3: Include topic produce/consume patterns**
- Document produce via SQL source-table writes and consume via `consumer`, `consumeBatch`, `ack`.

### Task 2: Rewrite README content

**Files:**
- Modify: `/Users/jamal/git/KalamDB/link/sdks/typescript/client/README.md`

**Step 1: Replace outdated sections and imports**
- Remove unsupported legacy API references.

**Step 2: Add complete sections**
- Installation, auth, lifecycle, queries, subscriptions, topic workflows, file uploads, helpers, types, wasm entrypoint.

**Step 3: Add complete API list**
- Keep method/type names aligned with current source.

### Task 3: Verify consistency before completion

**Files:**
- Modify as needed.

**Step 1: Run TypeScript build check for SDK package**
Run: `npm --prefix /Users/jamal/git/KalamDB/link/sdks/typescript/client run build:ts`
Expected: PASS.

**Step 2: Run README consistency grep checks**
Run: grep for stale/unsupported names removed (e.g., `ClientOptionsLegacy`, `subscribeMany`, deprecated constructor signatures).
Expected: no stale references.
