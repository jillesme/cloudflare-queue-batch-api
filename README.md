# Cloudflare Queue + Anthropic Batch API demo

A tiny Cloudflare Worker that takes user feedback over HTTP, drops it on a Cloudflare Queue, and uses the consumer batch to create a single [Anthropic Message Batch](https://docs.anthropic.com/en/api/creating-message-batches) job. A cron trigger polls Anthropic for results and writes them back to D1.

One Worker, three entry points:

- `fetch` — HTTP producer. Persists feedback to D1 and enqueues a job.
- `queue` — batched consumer. Turns up to N queued messages into one Anthropic batch request.
- `scheduled` — cron. Polls in-progress Anthropic batches and stores their results.

## Architecture

```
POST /feedback ──▶ D1 (queued) ──▶ Queue ──▶ queue()
                                                │
                             ┌──────────────────┴──────────────────┐
                  mode=sync  │                                     │  mode=batch
                             ▼                                     ▼
                   Anthropic /v1/messages              Anthropic /v1/messages/batches
                             │                                     │
                             ▼                                     ▼
                   D1 (completed / failed)           D1 (anthropic_submitted, in_progress)
                                                                   ▲
                                                                   │
                           cron (every minute) ──▶ Anthropic (retrieve) ──▶ D1 (completed / ...)

GET /api/feedback/:id ──▶ D1 ──▶ JSON (with elapsed_ms)
GET /                 ──▶ demo form with sync/batch toggle
```

Every submission goes through the queue. The queue still gives us buffering, retries, and a DLQ — what changes is whether the consumer calls Anthropic synchronously (fast, normal price) or submits a batch (slow, 50% off).

## Prerequisites

- Node.js 20+
- [pnpm](https://pnpm.io) (or swap for `npm` / `yarn`)
- A Cloudflare account with Workers, Queues, and D1 enabled
- An [Anthropic API key](https://console.anthropic.com/)

## Setup

1. **Install dependencies**

   ```sh
   pnpm install
   ```

2. **Create the D1 database** and copy the `database_id` it prints into `wrangler.jsonc`:

   ```sh
   pnpm wrangler d1 create feedback-analysis-db
   ```

3. **Create the queues** (the main work queue and a dead letter queue):

   ```sh
   pnpm wrangler queues create feedback-analysis-queue
   pnpm wrangler queues create feedback-analysis-dlq
   ```

4. **Apply migrations**:

   ```sh
   pnpm wrangler d1 migrations apply feedback-analysis-db --remote
   ```

   For local dev, run the same command with `--local` instead.

5. **Set your Anthropic API key** as a secret:

   ```sh
   pnpm wrangler secret put ANTHROPIC_API_KEY
   ```

6. **Generate types** (optional but recommended after any `wrangler.jsonc` changes):

   ```sh
   pnpm types
   ```

## Run locally

```sh
pnpm dev
```

Open <http://localhost:8787> for the demo form, or hit the API directly:

```sh
# Sync: one Messages API call per job, result in D1 within seconds.
curl -X POST http://localhost:8787/feedback \
  -H "content-type: application/json" \
  -d '{"source":"web_app","mode":"sync","text":"The dark mode toggle stopped working after the last update."}'

# Batch: joins the next queue batch, uses the Message Batches API (50% cheaper, minutes).
curl -X POST http://localhost:8787/feedback \
  -H "content-type: application/json" \
  -d '{"source":"web_app","mode":"batch","text":"..."}'
```

You'll get back:

```json
{ "ok": true, "status": "queued", "id": "…", "mode": "sync" }
```

Poll for the result:

```sh
curl http://localhost:8787/api/feedback/<id>
```

The response includes `elapsed_ms` — it ticks up while processing, then freezes at the real total on completion.

### Manually trigger the cron in local dev

`wrangler dev` doesn't run cron triggers on their schedule. If you submit a `batch`-mode job and want to poll Anthropic now, hit:

```sh
curl http://localhost:8787/cdn-cgi/handler/scheduled
```

## Deploy

```sh
pnpm deploy
```

## How it works

Every submission goes through the queue — the queue gives us buffering, retries, and a DLQ. What changes is what the consumer does with the message.

### Producer (`fetch`)

Validates the body (including `mode`), inserts a row with status `queued`, then `env.FEEDBACK_QUEUE.send(job)`. If enqueue fails, the row is marked `enqueue_failed`.

### Consumer (`queue`)

Cloudflare delivers up to `max_batch_size` messages together (or after `max_batch_timeout` seconds). The consumer splits the queue batch by `job.mode`:

- **`sync` jobs** → one `/v1/messages` call per job, in parallel. Result lands in D1 in 1-2 seconds per job. No cron needed. Normal pricing.
- **`batch` jobs** → one `/v1/messages/batches` call for the whole group. D1 rows are marked `anthropic_submitted` in a single `db.batch()` and the cron takes over. 50% cheaper, but Anthropic batches can take anywhere from a minute to a few hours.

If Anthropic returns an error, the affected messages are `retry()`ed with `QUEUE_RETRY_DELAY_SECONDS` delay. After `max_retries` they land in the DLQ.

### Poller (`scheduled`) — batch mode only

Runs every minute in production. Finds distinct `anthropic_batch_id`s that are still `in_progress`, retrieves each from Anthropic, and when a batch has `ended`, streams the JSONL results file and updates every affected row in a single `db.batch()` transaction.

Anthropic sometimes wraps JSON in code fences or emits trailing commentary. [`jsonrepair`](https://github.com/josdejong/jsonrepair) handles both without brittle regex.

## Configuration highlights

- **Queue consumer** — `max_batch_size: 3`, `max_batch_timeout: 60`, `max_retries: 3`, `dead_letter_queue: "feedback-analysis-dlq"`. Small batch size keeps the demo lively on video; bump to ~10 in production.
- **Cron** — `* * * * *` (every minute). This is the tightest Cloudflare allows and is ideal for a live demo. For production, 5 or 10 minutes is plenty — Anthropic batches can take up to 24 hours.
- **Retry delay** — 30 seconds. Short so failures recover quickly during a demo. A few minutes is saner in production.

## Files

```
src/index.ts                        # The entire Worker
migrations/0001_create_feedback_jobs.sql
wrangler.jsonc                      # Bindings, queues, cron
```

## Reset the demo

```sh
pnpm wrangler d1 execute feedback-analysis-db --remote \
  --command "DELETE FROM feedback_jobs"
```
