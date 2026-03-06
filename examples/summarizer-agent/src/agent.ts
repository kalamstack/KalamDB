import { config as loadEnv } from 'dotenv';
import {
  Auth,
  createClient,
  runAgent,
} from 'kalam-link';
import {
  buildGeminiAdapter,
  createSummarizerHandlers,
  normalizeUrlForNode,
} from './summarizer-runtime.js';

loadEnv({ path: '.env.local', quiet: true });
loadEnv({ quiet: true });

const KALAMDB_URL = normalizeUrlForNode(process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080');
const KALAMDB_USERNAME = process.env.KALAMDB_USERNAME ?? 'root';
const KALAMDB_PASSWORD = process.env.KALAMDB_PASSWORD ?? 'kalamdb123';
const TOPIC = process.env.KALAMDB_TOPIC ?? 'blog.summarizer';
const GROUP = process.env.KALAMDB_GROUP ?? 'blog-summarizer-agent';
const SYSTEM_PROMPT = process.env.KALAMDB_SYSTEM_PROMPT
  ?? 'Write one concise sentence summarizing the blog content. Preserve key facts and avoid hallucinations.';
const GEMINI_API_KEY = process.env.GEMINI_API_KEY?.trim() || process.env.GOOGLE_GENERATIVE_AI_API_KEY?.trim();
const GEMINI_MODEL = process.env.GEMINI_MODEL?.trim() || 'gemini-2.5-flash';

const client = createClient({
  url: KALAMDB_URL,
  authProvider: async () => Auth.basic(KALAMDB_USERNAME, KALAMDB_PASSWORD),
});

async function main(): Promise<void> {
  const llmAdapter = buildGeminiAdapter({
    geminiApiKey: GEMINI_API_KEY,
    geminiModel: GEMINI_MODEL,
  });
  const { onRow, onFailed } = createSummarizerHandlers();
  const abortController = new AbortController();

  const stop = (): void => {
    abortController.abort();
  };

  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);

  console.log(`summarizer-agent starting (topic=${TOPIC}, group=${GROUP})`);
  if (llmAdapter) {
    console.log(`summarizer-agent using Gemini model: ${GEMINI_MODEL}`);
  } else {
    console.log(
      'summarizer-agent using fallback summarizer (set GEMINI_API_KEY or GOOGLE_GENERATIVE_AI_API_KEY for Gemini)',
    );
  }

  try {
    await runAgent<Record<string, unknown>>({
      client,
      name: 'summarizer-agent',
      topic: TOPIC,
      groupId: GROUP,
      start: 'earliest',
      batchSize: 20,
      timeoutSeconds: 30,
      systemPrompt: SYSTEM_PROMPT,
      llm: llmAdapter,
      stopSignal: abortController.signal,
      retry: {
        maxAttempts: 3,
        initialBackoffMs: 250,
        maxBackoffMs: 1500,
        multiplier: 2,
      },
      onRow,
      onFailed,
      ackOnFailed: true,
      onRetry: ({ attempt, maxAttempts, runKey, error }) => {
        console.warn(
          `[retry] run_key=${runKey} attempt=${attempt}/${maxAttempts} error=${String(error)}`,
        );
      },
      onError: ({ runKey, error }) => {
        console.error(`[agent-error] run_key=${runKey} error=${String(error)}`);
      },
    });
  } finally {
    process.off('SIGINT', stop);
    process.off('SIGTERM', stop);
    await client.disconnect();
  }
}

main().catch((error) => {
  console.error('summarizer-agent failed:', error);
  process.exit(1);
});
