import { config as loadEnv } from 'dotenv';
import { fileURLToPath } from 'node:url';
import { Auth } from '@kalamdb/client';
import { createConsumerClient, runAgent } from '@kalamdb/consumer';

loadEnv({ path: '.env.local', quiet: true });
loadEnv({ quiet: true });

const KALAMDB_URL = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const KALAMDB_USERNAME = process.env.KALAMDB_USERNAME ?? 'admin';
const KALAMDB_PASSWORD = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';
const THINKING_DELAY_MS = 250;
const STREAM_DELAY_MS = 80;
const STREAM_CHUNK_SIZE = 18;

const TOPIC_NAME = 'chat_demo.ai_inbox';
const CONSUMER_GROUP = process.env.KALAMDB_GROUP ?? 'chat-ai-agent';
const CONSUMER_START = 'earliest';

type StartAgentOptions = {
  stopSignal?: AbortSignal;
};

type AgentEventStage = 'thinking' | 'typing' | 'message_saved' | 'complete' | 'log';

type ChatRow = Record<string, unknown> & {
  id: string;
  room: string;
  sender_username: string;
  content: string;
  role: string;
};

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
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

async function emitEvent(
  client: ReturnType<typeof createConsumerClient>,
  room: string,
  senderUsername: string,
  responseId: string,
  stage: AgentEventStage,
  preview: string,
  message: string,
): Promise<void> {
  const statement = [
    'INSERT INTO chat_demo.agent_events (response_id, room, sender_username, stage, preview, message)',
    `VALUES (${sqlLiteral(responseId)}, ${sqlLiteral(room)}, ${sqlLiteral(senderUsername)}, ${sqlLiteral(stage)}, ${sqlLiteral(preview)}, ${sqlLiteral(message)})`,
  ].join(' ');

  await client.executeAsUser(statement, assertValidUsername(senderUsername));
}

async function insertAssistantMessage(
  client: ReturnType<typeof createConsumerClient>,
  room: string,
  senderUsername: string,
  reply: string,
): Promise<void> {
  const statement = [
    'INSERT INTO chat_demo.messages (room, role, author, sender_username, content)',
    `VALUES (${sqlLiteral(room)}, 'assistant', 'KalamDB Copilot', ${sqlLiteral(senderUsername)}, ${sqlLiteral(reply)})`,
  ].join(' ');

  await client.executeAsUser(statement, assertValidUsername(senderUsername));
}

export function buildReply(content: string): string {
  const trimmed = content.trim();
  const lowered = trimmed.toLowerCase();
  let extra = 'This demo now uses runAgent() to consume new user messages from a topic — zero polling, zero wasted queries.';

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
  const client = createConsumerClient({
    url: KALAMDB_URL,
    authProvider: async () => Auth.basic(KALAMDB_USERNAME, KALAMDB_PASSWORD),
  });

  console.log(`chat-demo-agent ready (user=${KALAMDB_USERNAME}, mode=topic-consumer via runAgent)`);
  console.log(`  topic=${TOPIC_NAME}  group=${CONSUMER_GROUP}`);

  await runAgent<ChatRow>({
    client,
    name: 'chat-ai-agent',
    topic: TOPIC_NAME,
    groupId: CONSUMER_GROUP,
    start: CONSUMER_START,
    batchSize: 10,
    timeoutSeconds: 30,
    stopSignal: options.stopSignal,

    rowParser: (message) => {
      const payload = message.value as Record<string, unknown> | undefined;
      if (!payload) {
        return null;
      }

      // Topic CDC envelope: { row: { ... }, op: "INSERT", ... }
      const raw = (payload.row ?? payload) as Record<string, unknown>;
      const id = String(raw.id ?? '');
      const room = String(raw.room ?? 'main');
      const role = String(raw.role ?? '').trim();
      const senderUsername = String(raw.sender_username ?? '').trim();
      const content = String(raw.content ?? '').trim();

      if (!id || !senderUsername || !content || !role) {
        return null;
      }

      return { id, room, sender_username: senderUsername, content, role };
    },

    onRow: async (_ctx, row) => {
      if (row.role !== 'user') {
        console.log(`[agent] skipping non-user message id=${row.id} role=${row.role}`);
        return;
      }

      const responseId = `reply-${row.id}`;

      console.log(`[agent] received user message id=${row.id} user=${row.sender_username} room=${row.room}`);
      await emitEvent(client, row.room, row.sender_username, responseId, 'log', '', `Picked up user message ${row.id}`);
      await emitEvent(client, row.room, row.sender_username, responseId, 'thinking', '', 'Planning assistant reply');
      await sleep(THINKING_DELAY_MS);

      const reply = buildReply(row.content);
      let streamedReply = '';

      for (let index = 0; index < reply.length; index += STREAM_CHUNK_SIZE) {
        streamedReply += reply.slice(index, index + STREAM_CHUNK_SIZE);
        await emitEvent(client, row.room, row.sender_username, responseId, 'typing', streamedReply, `Streamed ${streamedReply.length}/${reply.length} characters`);
        await sleep(STREAM_DELAY_MS);
      }

      console.log(`[agent] committing assistant reply for user=${row.sender_username} message=${row.id}`);
      await emitEvent(client, row.room, row.sender_username, responseId, 'log', streamedReply, 'Persisting final assistant reply');
      await insertAssistantMessage(client, row.room, row.sender_username, reply);
      await emitEvent(client, row.room, row.sender_username, responseId, 'message_saved', reply, 'Assistant reply committed');
      await sleep(STREAM_DELAY_MS);
      await emitEvent(client, row.room, row.sender_username, responseId, 'complete', reply, 'Live stream finished');
      console.log(`[agent] completed reply for user=${row.sender_username} message=${row.id}`);
    },

    onError: ({ error, runKey }) => {
      console.error(`[agent] error processing ${runKey}:`, error);
    },

    onRetry: ({ attempt, maxAttempts, backoffMs, runKey }) => {
      console.warn(`[agent] retrying ${runKey} (attempt ${attempt}/${maxAttempts}, backoff ${backoffMs}ms)`);
    },
  });
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