import { config as loadEnv } from 'dotenv';
import { fileURLToPath } from 'node:url';
import { Auth } from '@kalamdb/client';
import { createConsumerClient, runAgent } from '@kalamdb/consumer';

loadEnv({ path: '.env.local', quiet: true });
loadEnv({ quiet: true });

const KALAMDB_URL = process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080';
const KALAMDB_USER = process.env.KALAMDB_USER ?? 'root';
const KALAMDB_PASSWORD = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';
const TOPIC = process.env.KALAMDB_TOPIC ?? 'blog.summarizer';
const GROUP = process.env.KALAMDB_GROUP ?? 'blog-summarizer-agent';

type StartAgentOptions = {
  stopSignal?: AbortSignal;
  groupId?: string;
  start?: 'latest' | 'earliest';
};

export function buildSummary(content: string): string {
  const compact = content.replace(/\s+/g, ' ').trim();
  const sentence = compact.split(/[.!?]/)[0]?.trim() ?? compact;
  const shortened = sentence.slice(0, 140).trim();
  return shortened.endsWith('.') ? shortened : `${shortened}.`;
}

export async function startSummarizerAgent(options: StartAgentOptions = {}): Promise<void> {
  const client = createConsumerClient({
    url: KALAMDB_URL,
    authProvider: async () => Auth.basic(KALAMDB_USER, KALAMDB_PASSWORD),
  });

  const groupId = options.groupId ?? GROUP;
  const start = options.start ?? 'latest';

  console.log(`summarizer-agent ready (topic=${TOPIC}, group=${groupId})`);

  try {
    await runAgent<Record<string, unknown>>({
      client,
      name: 'summarizer-agent',
      topic: TOPIC,
      groupId,
      start,
      stopSignal: options.stopSignal,
      retry: { maxAttempts: 3, initialBackoffMs: 250, maxBackoffMs: 1500 },
      onRow: async (ctx, row) => {
        const blogId = row.blog_id ?? row.blogId;
        const content = String(row.content ?? '').trim();
        const currentSummary = String(row.summary ?? '').trim();

        if (!blogId || !content) {
          return;
        }

        const nextSummary = buildSummary(content);
        if (currentSummary === nextSummary) {
          return;
        }

        await ctx.sql(
          'UPDATE blog.blogs SET summary = $1, updated = NOW() WHERE blog_id = $2',
          [nextSummary, blogId],
        );
      },
      onFailed: async (ctx) => {
        await ctx.sql(
          'INSERT INTO blog.summary_failures (run_key, blog_id, error) VALUES ($1, $2, $3)',
          [ctx.runKey, String(ctx.row.blog_id ?? 'unknown'), String(ctx.error ?? 'unknown')],
        );
      },
      ackOnFailed: true,
      onError: ({ error }) => {
        console.error('summarizer-agent error', error);
      },
    });
  } finally {
    await client.disconnect();
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === process.argv[1]) {
  const controller = new AbortController();
  process.on('SIGINT', () => controller.abort());
  process.on('SIGTERM', () => controller.abort());

  startSummarizerAgent({ stopSignal: controller.signal }).catch((error) => {
    console.error('summarizer-agent failed:', error);
    process.exit(1);
  });
}
