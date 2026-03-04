import { createHash } from 'node:crypto';
import { ChatGoogleGenerativeAI } from '@langchain/google-genai';
import {
  createLangChainAdapter,
  type AgentContext,
  type AgentFailureContext,
  type AgentLLMAdapter,
  type RunAgentOptions,
} from 'kalam-link';

type AgentRow = Record<string, unknown>;

const MAX_TRACKED_BLOGS = 10_000;

export function normalizeUrlForNode(url: string): string {
  const parsed = new URL(url);
  if (parsed.hostname === 'localhost') {
    parsed.hostname = '127.0.0.1';
  }
  return parsed.toString().replace(/\/$/, '');
}

export function buildGeminiAdapter(args: {
  geminiApiKey?: string;
  geminiModel: string;
}): AgentLLMAdapter | undefined {
  if (!args.geminiApiKey) {
    return undefined;
  }

  const model = new ChatGoogleGenerativeAI({
    apiKey: args.geminiApiKey,
    model: args.geminiModel,
    temperature: 0,
  });

  return createLangChainAdapter(model);
}

function fallbackSummary(content: string): string {
  const compact = content.replace(/\s+/g, ' ').trim();
  if (!compact) {
    return '';
  }

  const words = compact.split(' ');
  const excerpt = words.slice(0, 24).join(' ');
  return words.length > 24 ? `${excerpt}...` : excerpt;
}

function contentFingerprint(content: string): string {
  return createHash('sha256').update(content).digest('hex');
}

function toBlogId(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }
  if (typeof value === 'string' && value.trim() !== '') {
    return value.trim();
  }
  return null;
}

async function summarizeContent(ctx: AgentContext<AgentRow>, content: string): Promise<string> {
  const compact = content.replace(/\s+/g, ' ').trim();
  if (!compact) {
    return '';
  }

  if (!ctx.llm) {
    return fallbackSummary(compact);
  }

  const prompt = `Summarize the following blog post in one short sentence:\n\n${compact}`;
  const generated = (await ctx.llm.complete(prompt)).trim();
  return generated || fallbackSummary(compact);
}

async function persistFailure(
  ctx: AgentFailureContext<AgentRow>,
  failedBlogId: string,
  safeError: string,
): Promise<void> {
  const updateResult = await ctx.sql(
    `
    UPDATE blog.summary_failures
    SET blog_id = $2, error = $3, updated = NOW()
    WHERE run_key = $1
    `,
    [ctx.runKey, failedBlogId, safeError],
  );

  const updatedRows = Number(updateResult.results?.[0]?.row_count ?? 0);
  if (updatedRows > 0) {
    console.warn(`[failed-sink] run_key=${ctx.runKey} action=update blog_id=${failedBlogId}`);
    return;
  }

  await ctx.sql(
    `
    INSERT INTO blog.summary_failures (run_key, blog_id, error, created, updated)
    VALUES ($1, $2, $3, NOW(), NOW())
    `,
    [ctx.runKey, failedBlogId, safeError],
  );
  console.warn(`[failed-sink] run_key=${ctx.runKey} action=insert blog_id=${failedBlogId}`);
}

export function createSummarizerHandlers():
Pick<RunAgentOptions<AgentRow>, 'onRow' | 'onFailed'> {
  const lastProcessedContentFingerprint = new Map<string, string>();

  const rememberContentFingerprint = (blogId: string, fingerprint: string): void => {
    if (lastProcessedContentFingerprint.size >= MAX_TRACKED_BLOGS) {
      const firstKey = lastProcessedContentFingerprint.keys().next().value;
      if (typeof firstKey === 'string') {
        lastProcessedContentFingerprint.delete(firstKey);
      }
    }
    lastProcessedContentFingerprint.set(blogId, fingerprint);
  };

  const updateSummary = async (
    ctx: AgentContext<AgentRow>,
    blogId: string,
  ): Promise<void> => {
    console.log(`[wake] run_key=${ctx.runKey} blog_id=${blogId} attempt=${ctx.attempt}/${ctx.maxAttempts}`);

    const row = await ctx.queryOne(
      'SELECT blog_id, content, summary FROM blog.blogs WHERE blog_id = $1',
      [blogId],
    );

    const content = row?.['content']?.asString();
    if (!row || !content) {
      console.warn(`[skip] run_key=${ctx.runKey} blog_id=${blogId} reason=missing-row-or-content`);
      return;
    }

    const compactContent = content.replace(/\s+/g, ' ').trim();
    if (!compactContent) {
      console.warn(`[skip] run_key=${ctx.runKey} blog_id=${blogId} reason=empty-content`);
      return;
    }

    const fingerprint = contentFingerprint(compactContent);
    const previousFingerprint = lastProcessedContentFingerprint.get(blogId);
    if (previousFingerprint === fingerprint) {
      console.log(`[skip] run_key=${ctx.runKey} blog_id=${blogId} reason=content-unchanged-since-last-run`);
      return;
    }

    console.log(`[process] run_key=${ctx.runKey} blog_id=${blogId} action=summarize`);
    const nextSummary = await summarizeContent(ctx, compactContent);
    if (!nextSummary) {
      console.warn(`[skip] run_key=${ctx.runKey} blog_id=${blogId} reason=empty-summary`);
      return;
    }

    const currentSummary = (row['summary']?.asString() ?? '').trim();
    if (currentSummary === nextSummary) {
      rememberContentFingerprint(blogId, fingerprint);
      console.log(`[skip] run_key=${ctx.runKey} blog_id=${blogId} reason=unchanged-summary`);
      return;
    }

    await ctx.sql(
      'UPDATE blog.blogs SET summary = $1, updated = NOW() WHERE blog_id = $2',
      [nextSummary, blogId],
    );
    rememberContentFingerprint(blogId, fingerprint);

    console.log(`[summarized] run_key=${ctx.runKey} blog_id=${blogId}`);
  };

  const onRow: RunAgentOptions<AgentRow>['onRow'] = async (ctx, row): Promise<void> => {
    console.log(
      `[event] run_key=${ctx.runKey} partition=${ctx.message.partition_id} offset=${ctx.message.offset} topic=${ctx.message.topic}`,
    );

    const blogId = toBlogId(row.blog_id);
    if (!blogId) {
      console.warn(`[skip] run_key=${ctx.runKey} reason=invalid-blog-id`);
      return;
    }

    await updateSummary(ctx, blogId);
  };

  const onFailed: RunAgentOptions<AgentRow>['onFailed'] = async (ctx): Promise<void> => {
    const failedBlogId = toBlogId(ctx.row.blog_id) ?? 'unknown';
    const errorText = String(ctx.error instanceof Error ? ctx.error.message : ctx.error ?? 'unknown');
    await persistFailure(ctx, failedBlogId, errorText.slice(0, 4000));
  };

  return { onRow, onFailed };
}
