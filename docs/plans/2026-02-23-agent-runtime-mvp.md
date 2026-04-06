# Agent Runtime MVP Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a minimal, production-usable TypeScript `runAgent` API in `@kalamdb/consumer` and migrate `examples/summarizer-agent` to depend on that API with local SDK loading and verified end-to-end execution.

**Architecture:** Keep Rust/WASM transport as the single low-level source of truth (`consume`, `ack`, SQL). Add a thin TypeScript runtime layer that orchestrates consume loop behavior (retry, ack, failure handling, and system prompt plumbing) without duplicating transport logic. Update the summarizer example to use only high-level SDK APIs and a local SDK dependency path.

**Tech Stack:** TypeScript (ESM), Node.js 18+, `@kalamdb/consumer` + `@kalamdb/client`, tsx runtime, bash setup scripts, KalamDB SQL topic APIs.

---

### Task 1: Add SDK Agent Runtime Types

**Files:**
- Create: `link/sdks/typescript/consumer/src/agent.ts`
- Modify: `link/sdks/typescript/consumer/src/types.ts`
- Modify: `link/sdks/typescript/consumer/src/index.ts`

**Step 1: Write the failing type import usage (compile-level check)**

Use expected API shape in temporary in-repo snippet:

```typescript
import { runAgent, type AgentContext } from '@kalamdb/consumer';
```

**Step 2: Run type build to verify it fails before implementation**

Run: `cd link/sdks/typescript/consumer && npm run build:ts`
Expected: Type export errors for `runAgent` / `AgentContext`.

**Step 3: Write minimal runtime types**

Implement:
- `RunAgentOptions`
- `AgentContext`
- `AgentRetryPolicy`
- `AgentFailureHandler`
- `AgentSystemPromptConfig`

**Step 4: Export these types from public SDK surface**

Wire exports through `src/types.ts` and `src/index.ts`.

**Step 5: Re-run type build**

Run: `cd link/sdks/typescript/consumer && npm run build:ts`
Expected: Type build passes.

**Step 6: Commit**

```bash
git add link/sdks/typescript/consumer/src/agent.ts link/sdks/typescript/consumer/src/types.ts link/sdks/typescript/consumer/src/index.ts
git commit -m "feat(ts-sdk): add agent runtime types and exports"
```

### Task 2: Implement `runAgent` Retry/Ack Runtime

**Files:**
- Modify: `link/sdks/typescript/consumer/src/agent.ts`
- Modify: `link/sdks/typescript/consumer/src/client.ts`
- Modify: `link/sdks/typescript/consumer/src/types.ts`

**Step 1: Write failing behavior test scaffold (logic-level)**

Create minimal assertions around:
- no ack before success,
- bounded retries,
- ack after failure sink success,
- no ack when failure sink fails.

**Step 2: Run test/build to verify failure**

Run: `cd link/sdks/typescript/consumer && npm run build:ts`
Expected: compile/test fails until runtime functions are implemented.

**Step 3: Implement runtime loop**

Implement in `runAgent`:
- create consumer with `auto_ack: false`,
- invoke handler with `ctx.sql`, `ctx.ack`, `ctx.runKey`, `ctx.systemPrompt`,
- retry per policy,
- call failure handler after retries exhausted,
- ack only on success path or successful failure handling.

**Step 4: Keep API minimal**

No extra framework abstractions in MVP. One function + optional hooks.

**Step 5: Run build verification**

Run: `cd link/sdks/typescript/consumer && npm run build`
Expected: full build succeeds (`wasm`, typescript, copy).

**Step 6: Commit**

```bash
git add link/sdks/typescript/consumer/src/agent.ts link/sdks/typescript/consumer/src/client.ts link/sdks/typescript/consumer/src/types.ts
git commit -m "feat(ts-sdk): add runAgent with retry and ack semantics"
```

### Task 3: Migrate `summarizer-agent` to SDK-Only Minimal API

**Files:**
- Modify: `examples/summarizer-agent/src/agent.ts`
- Modify: `examples/summarizer-agent/package.json`
- Create: `examples/summarizer-agent/scripts/ensure-sdk.sh`
- Modify: `examples/summarizer-agent/README.md`
- Modify: `examples/summarizer-agent/.env.example`

**Step 1: Write failing run command expectation**

Run current app with clean install:

```bash
cd examples/summarizer-agent
npm install
npm run start
```

Expected: startup failure or old consumer boilerplate usage remains.

**Step 2: Replace raw consumer loop with `runAgent`**

Implement minimal agent:
- consume `blog.summarizer`,
- read row by `blog_id`,
- generate summary,
- update row.

Use optional `SYSTEM_PROMPT` env to show future LLM alignment.

**Step 3: Add local SDK ensure hook**

Add `presetup/prestart` script that builds local SDK when missing/outdated.

**Step 4: Align env style with chat-with-ai**

Preserve `.env.local` loading pattern and documented vars.

**Step 5: Verify static checks**

Run: `cd examples/summarizer-agent && npm run check`
Expected: TypeScript check passes.

**Step 6: Commit**

```bash
git add examples/summarizer-agent/src/agent.ts examples/summarizer-agent/package.json examples/summarizer-agent/scripts/ensure-sdk.sh examples/summarizer-agent/README.md examples/summarizer-agent/.env.example
git commit -m "feat(example): simplify summarizer-agent with runAgent"
```

### Task 4: Harden Setup Script + SQL Bootstrap

**Files:**
- Modify: `examples/summarizer-agent/setup.sh`
- Modify: `examples/summarizer-agent/setup.sql`

**Step 1: Write failing setup scenario**

Run: `cd examples/summarizer-agent && ./setup.sh`
Expected: capture failure details for topic/table bootstrap edge cases.

**Step 2: Ensure idempotent SQL and robust parser behavior**

Ensure setup script can safely re-run and gracefully skip already-existing entities.

**Step 3: Generate `.env.local` in chat-with-ai style**

Keep variable naming and comments aligned with existing example conventions.

**Step 4: Re-run setup validation**

Run: `cd examples/summarizer-agent && ./setup.sh`
Expected: success with printed sample `blog_id`.

**Step 5: Commit**

```bash
git add examples/summarizer-agent/setup.sh examples/summarizer-agent/setup.sql
git commit -m "fix(example): make summarizer setup idempotent and reliable"
```

### Task 5: End-to-End Verification (Must Pass Before Completion)

**Files:**
- Runtime validation only (no mandatory file changes)

**Step 1: Start backend server**

Run: `cd backend && cargo run`
Expected: server healthy at `http://127.0.0.1:8080`.

**Step 2: Bootstrap and start agent**

Run:

```bash
cd examples/summarizer-agent
./setup.sh
npm install
npm run start
```

Expected: agent starts with no WASM fetch/path errors.

**Step 3: Trigger an UPDATE event and verify summary write-back**

Run SQL update + select via API/CLI.
Expected: `summary` changes after event processing.

**Step 4: Record exact verification evidence in README notes**

Include one concrete run transcript snippet and troubleshooting note.

**Step 5: Final commit**

```bash
git add examples/summarizer-agent/README.md
git commit -m "docs(example): add verified run notes for summarizer agent"
```
