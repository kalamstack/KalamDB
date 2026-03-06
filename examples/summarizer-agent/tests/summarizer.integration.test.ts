import { execFileSync } from 'node:child_process';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as sleep } from 'node:timers/promises';
import { Auth, createClient } from 'kalam-link';
import { buildSummary, startSummarizerAgent } from '../src/agent.js';

const exampleRoot = path.resolve(process.cwd());

test('agent writes summaries back into blog.blogs', async () => {
  execFileSync('./setup.sh', [], {
    cwd: exampleRoot,
    stdio: 'inherit',
    env: {
      ...process.env,
      KALAMDB_URL: process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080',
    },
  });

  const controller = new AbortController();
  const agentRun = startSummarizerAgent({
    stopSignal: controller.signal,
    groupId: `blog-summarizer-test-${Date.now()}`,
    start: 'earliest',
  });
  await sleep(750);

  const client = createClient({
    url: process.env.KALAMDB_URL ?? 'http://127.0.0.1:8080',
    authProvider: async () => Auth.basic('root', process.env.KALAMDB_PASSWORD ?? 'kalamdb123'),
  });

  const content = `KalamDB topics wake lightweight workers immediately after a row changes ${Date.now()}. The worker can enrich the row without polling.`;
  const expected = buildSummary(content);

  try {
    await client.query('INSERT INTO blog.blogs (content, summary) VALUES ($1, $2)', [content, null]);

    let summary: string | null = null;
    const deadline = Date.now() + 20_000;

    while (Date.now() < deadline) {
      const row = await client.queryOne(
        'SELECT blog_id, summary FROM blog.blogs WHERE content = $1 ORDER BY created DESC LIMIT 1',
        [content],
      );
      summary = row?.summary?.asString() ?? null;
      if (summary) {
        break;
      }
      await sleep(400);
    }

    assert.equal(summary, expected);
  } finally {
    controller.abort();
    await Promise.race([agentRun, sleep(3_000)]);
    await client.disconnect();
  }
});