# Summarizer Agent

This is the worker-only example in the set.

It demonstrates the smallest useful `runAgent()` flow:

1. a row lands in `blog.blogs`
2. KalamDB routes the change into `blog.summarizer`
3. the worker consumes the topic
4. the worker writes `summary` back into the same row
5. failures are stored in `blog.summary_failures`

There is no model dependency here. The summarizer is deterministic so the example is easy to read and the test is stable.

## Quick start

1. Make sure KalamDB is running on `http://127.0.0.1:8080`.
2. Run `npm install`.
3. Run `npm run setup`.
4. Run `npm run start`.

## Test it

Run `npm test`.

The integration test starts the agent, inserts a blog row, and waits until the summary field is written back by the worker.

## Files worth reading

- `src/agent.ts`: full worker logic
- `setup.sql`: schema and topic route
- `tests/summarizer.integration.test.ts`: end-to-end verification
