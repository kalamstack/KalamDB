import { config as loadEnv } from 'dotenv';
import { fileURLToPath } from 'node:url';
import { Auth, createClient, type RowData } from 'kalam-link';

loadEnv({ path: '.env.local', quiet: true });
loadEnv({ quiet: true });

const KALAMDB_URL = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const KALAMDB_USERNAME = process.env.KALAMDB_USERNAME ?? 'admin';
const KALAMDB_PASSWORD = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';
const POLL_INTERVAL_MS = 200;
const THINKING_DELAY_MS = 250;
const STREAM_DELAY_MS = 80;
const STREAM_CHUNK_SIZE = 18;

type StartAgentOptions = {
  stopSignal?: AbortSignal;
};

type StringCellLike = {
  asString: () => string | null;
};

type IntCellLike = {
  asInt: () => number | null;
};

type PendingMessage = {
  id: string;
  room: string;
  senderUsername: string;
  content: string;
};

type AgentEventStage = 'thinking' | 'typing' | 'message_saved' | 'complete' | 'log';

function isStringCellLike(value: unknown): value is StringCellLike {
  return Boolean(value) && typeof value === 'object' && typeof (value as StringCellLike).asString === 'function';
}

function isIntCellLike(value: unknown): value is IntCellLike {
  return Boolean(value) && typeof value === 'object' && typeof (value as IntCellLike).asInt === 'function';
}

function text(row: RowData, key: string): string {
  return isStringCellLike(row[key]) ? row[key].asString() ?? '' : '';
}

function int(row: RowData, key: string): number {
  return isIntCellLike(row[key]) ? row[key].asInt() ?? 0 : 0;
}

export function toPendingMessage(row: RowData): PendingMessage | null {
  const id = text(row, 'id');
  const room = text(row, 'room') || 'main';
  const senderUsername = text(row, 'sender_username').trim();
  const content = text(row, 'content').trim();
  const createdAt = int(row, 'created_at');

  if (!id || !senderUsername || !content || createdAt <= 0) {
    return null;
  }

  return { id, room, senderUsername, content };
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function initialWatermark(client: ReturnType<typeof createClient>): Promise<PendingMessage | null> {
  const row = await client.queryOne(
    "SELECT id, room, sender_username, content, created_at FROM chat_demo.messages WHERE role = 'user' ORDER BY id DESC LIMIT 1",
  );

  return row ? toPendingMessage(row) : null;
}

function assertValidUsername(username: string): string {
  if (!/^[A-Za-z0-9._-]+$/.test(username)) {
    throw new Error(`Unsupported username for EXECUTE AS USER: ${username}`);
  }

  return username;
}

function sqlLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function buildExecuteAsUser(targetUsername: string, statement: string): string {
  return `EXECUTE AS USER '${assertValidUsername(targetUsername)}' (${statement})`;
}

async function emitEvent(
  client: ReturnType<typeof createClient>,
  pending: PendingMessage,
  responseId: string,
  stage: AgentEventStage,
  preview: string,
  message: string,
): Promise<void> {
  const statement = [
    'INSERT INTO chat_demo.agent_events (response_id, room, sender_username, stage, preview, message)',
    `VALUES (${sqlLiteral(responseId)}, ${sqlLiteral(pending.room)}, ${sqlLiteral(pending.senderUsername)}, ${sqlLiteral(stage)}, ${sqlLiteral(preview)}, ${sqlLiteral(message)})`,
  ].join(' ');

  await client.query(buildExecuteAsUser(pending.senderUsername, statement));
}

async function insertAssistantMessage(
  client: ReturnType<typeof createClient>,
  pending: PendingMessage,
  reply: string,
): Promise<void> {
  const statement = [
    'INSERT INTO chat_demo.messages (room, role, author, sender_username, content)',
    `VALUES (${sqlLiteral(pending.room)}, 'assistant', 'KalamDB Copilot', ${sqlLiteral(pending.senderUsername)}, ${sqlLiteral(reply)})`,
  ].join(' ');

  await client.query(buildExecuteAsUser(pending.senderUsername, statement));
}

async function processMessage(
  client: ReturnType<typeof createClient>,
  pending: PendingMessage,
): Promise<void> {
  const responseId = `reply-${pending.id}`;

  console.log(`[agent] received user message id=${pending.id} user=${pending.senderUsername} room=${pending.room}`);
  await emitEvent(client, pending, responseId, 'log', '', `Picked up user message ${pending.id}`);
  await emitEvent(client, pending, responseId, 'thinking', '', 'Planning assistant reply');
  await sleep(THINKING_DELAY_MS);

  const reply = buildReply(pending.content);
  let streamedReply = '';

  for (let index = 0; index < reply.length; index += STREAM_CHUNK_SIZE) {
    streamedReply += reply.slice(index, index + STREAM_CHUNK_SIZE);
    await emitEvent(client, pending, responseId, 'typing', streamedReply, `Streamed ${streamedReply.length}/${reply.length} characters`);
    await sleep(STREAM_DELAY_MS);
  }

  console.log(`[agent] committing assistant reply for user=${pending.senderUsername} message=${pending.id}`);
  await emitEvent(client, pending, responseId, 'log', streamedReply, 'Persisting final assistant reply');
  await insertAssistantMessage(client, pending, reply);
  await emitEvent(client, pending, responseId, 'message_saved', reply, 'Assistant reply committed');
  await sleep(STREAM_DELAY_MS);
  await emitEvent(client, pending, responseId, 'complete', reply, 'Live stream finished');
  console.log(`[agent] completed reply for user=${pending.senderUsername} message=${pending.id}`);
}

export function buildReply(content: string): string {
  const trimmed = content.trim();
  const lowered = trimmed.toLowerCase();
  let extra = 'This demo now writes your persisted chat rows into a USER table and streams the live drafting state through a STREAM table.';

  if (lowered.includes('latency')) {
    extra = 'I would inspect the slowest route first, then compare the latest write volume against the baseline you see in the database.';
  } else if (lowered.includes('deploy')) {
    extra = 'That sounds deploy-shaped. I would compare the newest release marker against the first spike in user messages.';
  } else if (lowered.includes('queue')) {
    extra = 'Queue growth usually means a downstream dependency is flattening. This is a good fit for another agent wired to a different topic.';
  }

  return `AI reply: KalamDB stored "${trimmed}" in chat_demo.messages, streamed the drafting state through chat_demo.agent_events, and committed the final reply with EXECUTE AS USER. ${extra}`;
}

export async function startChatAgent(options: StartAgentOptions = {}): Promise<void> {
  const client = createClient({
    url: KALAMDB_URL,
    authProvider: async () => Auth.basic(KALAMDB_USERNAME, KALAMDB_PASSWORD),
  });

  console.log(`chat-demo-agent ready (user=${KALAMDB_USERNAME}, mode=user-table-poller)`);

  try {
    let watermark = await initialWatermark(client);

    while (!options.stopSignal?.aborted) {
      const rows = watermark
        ? await client.queryAll(
          "SELECT id, room, sender_username, content, created_at FROM chat_demo.messages WHERE role = 'user' AND id > CAST($1 AS BIGINT) ORDER BY id ASC LIMIT 25",
          [watermark.id],
        )
        : await client.queryAll(
          "SELECT id, room, sender_username, content, created_at FROM chat_demo.messages WHERE role = 'user' ORDER BY id ASC LIMIT 25",
        );

      for (const row of rows) {
        const pending = toPendingMessage(row);
        if (!pending) {
          continue;
        }

        await processMessage(client, pending);
        watermark = pending;
      }

      await sleep(POLL_INTERVAL_MS);
    }
  } finally {
    await client.disconnect();
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const controller = new AbortController();
  process.on('SIGINT', () => controller.abort());
  process.on('SIGTERM', () => controller.abort());

  startChatAgent({ stopSignal: controller.signal }).catch((error) => {
    console.error('chat-demo-agent failed', error);
    process.exit(1);
  });
}