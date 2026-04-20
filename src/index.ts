import { jsonrepair } from "jsonrepair";

const FEEDBACK_CATEGORIES = [
  "bug",
  "feature_request",
  "praise",
  "billing",
  "other",
] as const;
const FEEDBACK_SOURCES = [
  "web_app",
  "support_email",
  "app_store",
  "sales_call",
] as const;
const FEEDBACK_PRIORITIES = ["low", "medium", "high"] as const;
const FEEDBACK_MODES = ["batch", "sync"] as const;
const TERMINAL_STATUSES = [
  "completed",
  "failed",
  "canceled",
  "expired",
  "enqueue_failed",
] as const;

type FeedbackCategory = (typeof FEEDBACK_CATEGORIES)[number];
type FeedbackSource = (typeof FEEDBACK_SOURCES)[number];
type FeedbackPriority = (typeof FEEDBACK_PRIORITIES)[number];
type FeedbackMode = (typeof FEEDBACK_MODES)[number];

interface FeedbackJob {
  id: string;
  source: FeedbackSource;
  text: string;
  submittedAt: string;
  // `batch` uses Anthropic Message Batches (cheaper, async, cron-polled).
  // `sync` calls the Messages API once per job (fast, normal price). Great for
  // interactive flows or demos where you want to see the result now.
  mode: FeedbackMode;
}

interface FeedbackRequestBody {
  source?: unknown;
  text?: unknown;
  mode?: unknown;
}

interface FeedbackRow {
  id: string;
  source: string;
  text: string;
  submitted_at: string;
  status: string;
  anthropic_batch_id: string | null;
  anthropic_processing_status: string | null;
  category: string | null;
  priority: string | null;
  summary: string | null;
  result_json: string | null;
  error_text: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

interface AnthropicBatchRequest {
  custom_id: string;
  params: {
    model: string;
    max_tokens: number;
    system: string;
    messages: Array<{
      role: "user";
      content: string;
    }>;
  };
}

interface AnthropicBatchCreateResponse {
  id: string;
  processing_status: string;
}

interface AnthropicBatchRetrieveResponse {
  id: string;
  processing_status: "in_progress" | "canceling" | "ended";
  results_url?: string;
  request_counts?: {
    processing: number;
    succeeded: number;
    errored: number;
    canceled: number;
    expired: number;
  };
}

interface AnthropicSucceededResult {
  type: "succeeded";
  message: {
    content: Array<{
      type: string;
      text?: string;
    }>;
  };
}

interface AnthropicErroredResult {
  type: "errored";
  error?: {
    error?: {
      type?: string;
      message?: string;
    };
  };
}

interface AnthropicCanceledResult {
  type: "canceled";
}

interface AnthropicExpiredResult {
  type: "expired";
}

interface AnthropicBatchResultLine {
  custom_id: string;
  result:
    | AnthropicSucceededResult
    | AnthropicErroredResult
    | AnthropicCanceledResult
    | AnthropicExpiredResult;
}

interface FeedbackAnalysis {
  category: FeedbackCategory;
  priority: FeedbackPriority;
  summary: string;
}

interface WorkerEnv {
  ANTHROPIC_API_KEY: string;
  FEEDBACK_QUEUE: Queue<FeedbackJob>;
  FEEDBACK_DB: D1Database;
}

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages/batches";
const ANTHROPIC_API_VERSION = "2023-06-01";
const ANTHROPIC_MODEL = "claude-haiku-4-5";

// Short so that a failed Anthropic call recovers quickly during a demo.
// Bump to a few minutes in production.
const QUEUE_RETRY_DELAY_SECONDS = 30;

const SYSTEM_PROMPT = [
  "You analyze user feedback for a product team.",
  "Classify the feedback into exactly one category: bug, feature_request, praise, billing, or other.",
  "Assign exactly one priority: low, medium, or high.",
  "Return compact JSON with keys category, priority, and summary.",
  "The summary must be a short sentence under 20 words.",
].join(" ");

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    const url = new URL(request.url);
    const { pathname } = url;

    if (request.method === "GET" && pathname === "/") {
      return html(renderDemoPage());
    }

    if (request.method === "GET" && pathname.startsWith("/api/feedback/")) {
      const id = pathname.slice("/api/feedback/".length).trim();
      const row = id ? await getFeedbackRow(env.FEEDBACK_DB, id) : null;

      if (!row) {
        return json({ ok: false, error: "Feedback item not found" }, 404);
      }

      return json({ ok: true, feedback: normalizeFeedbackRow(row) });
    }

    if (request.method === "POST" && pathname === "/feedback") {
      return handleFeedbackSubmission(request, env);
    }

    return json(
      {
        ok: false,
        error:
          "Use GET / for the demo form, GET /api/feedback/:id for status, or POST /feedback with JSON: { source, text }",
      },
      404,
    );
  },

  async queue(batch: MessageBatch<FeedbackJob>, env: WorkerEnv): Promise<void> {
    console.log("Queue consumer received batch", {
      messageCount: batch.messages.length,
      ids: batch.messages.map((message) => message.id),
    });

    // Parse every message up front. If any single message is malformed, ack
    // only that one and keep going with the rest of the batch.
    const batchJobs: { message: Message<FeedbackJob>; job: FeedbackJob }[] = [];
    for (const message of batch.messages) {
      try {
        batchJobs.push({ message, job: asFeedbackJob(message.body) });
      } catch (error) {
        console.error("Queue message is not a valid FeedbackJob, acking", {
          messageId: message.id,
          error: asErrorMessage(error),
        });
        message.ack();
      }
    }

    // Split by mode. Batch-mode jobs become one Anthropic Message Batch;
    // sync-mode jobs fan out to individual /v1/messages calls in parallel.
    const batchModeJobs = batchJobs.filter(({ job }) => job.mode === "batch");
    const syncModeJobs = batchJobs.filter(({ job }) => job.mode === "sync");

    await Promise.all([
      batchModeJobs.length > 0 ? processBatchMode(batchModeJobs, env) : Promise.resolve(),
      syncModeJobs.length > 0 ? processSyncMode(syncModeJobs, env) : Promise.resolve(),
    ]);
  },

  async scheduled(controller: ScheduledController, env: WorkerEnv): Promise<void> {
    console.log("Cron poll started", {
      cron: controller.cron,
      scheduledTime: new Date(controller.scheduledTime).toISOString(),
    });

    const batchIds = await getInProgressBatchIds(env.FEEDBACK_DB);

    if (batchIds.length === 0) {
      console.log("Cron poll found no in-progress Anthropic batches");
      return;
    }

    console.log("Cron poll checking Anthropic batches", { batchIds });

    for (const batchId of batchIds) {
      await pollAnthropicBatch(env, batchId);
    }
  },
} satisfies ExportedHandler<WorkerEnv, FeedbackJob>;

async function handleFeedbackSubmission(
  request: Request,
  env: WorkerEnv,
): Promise<Response> {
  let body: FeedbackRequestBody;

  try {
    body = (await request.json()) as FeedbackRequestBody;
  } catch {
    return json({ ok: false, error: "Invalid JSON body" }, 400);
  }

  const source = asFeedbackSource(body.source);
  const text = asNonEmptyString(body.text);
  const mode = asFeedbackMode(body.mode) ?? "batch";

  if (!source || !text) {
    return json(
      {
        ok: false,
        error:
          "`source` must be one of the allowed options and `text` must be a non-empty string",
      },
      400,
    );
  }

  const job: FeedbackJob = {
    id: crypto.randomUUID(),
    source,
    text,
    submittedAt: new Date().toISOString(),
    mode,
  };

  await insertFeedbackJob(env.FEEDBACK_DB, job);

  console.log("Received feedback submission", {
    id: job.id,
    source: job.source,
    mode: job.mode,
    textLength: job.text.length,
  });

  try {
    // Producer: accept user feedback over HTTP, persist it to D1, then enqueue
    // a typed job for async queue-based processing. The consumer picks the
    // right downstream path (batch vs sync) based on `job.mode`.
    await env.FEEDBACK_QUEUE.send(job);

    console.log("Enqueued feedback job", {
      id: job.id,
      source: job.source,
      mode: job.mode,
    });

    return json({ ok: true, status: "queued", id: job.id, mode: job.mode });
  } catch (error) {
    await markEnqueueFailed(env.FEEDBACK_DB, job.id, asErrorMessage(error));
    console.error("Failed to enqueue feedback job", {
      id: job.id,
      error: asErrorMessage(error),
    });
    return json({ ok: false, error: "Failed to enqueue feedback" }, 500);
  }
}

async function processBatchMode(
  batchJobs: { message: Message<FeedbackJob>; job: FeedbackJob }[],
  env: WorkerEnv,
): Promise<void> {
  // Turn the queue batch into one Anthropic Message Batch request.
  // Cheaper (50% discount) but asynchronous — the cron handler picks up results later.
  const jobs = batchJobs.map(({ job }) => job);
  const requests = jobs.map(toAnthropicRequest);

  try {
    const response = await fetch(ANTHROPIC_API_URL, {
      method: "POST",
      headers: anthropicHeaders(env.ANTHROPIC_API_KEY),
      body: JSON.stringify({ requests }),
    });

    if (!response.ok) {
      console.error("Anthropic batch creation failed", {
        status: response.status,
        // Body may echo request fields; do NOT include request headers.
        body: await response.text(),
      });
      for (const { message } of batchJobs) {
        message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
      }
      return;
    }

    const createdBatch = (await response.json()) as AnthropicBatchCreateResponse;

    await markJobsSubmitted(
      env.FEEDBACK_DB,
      jobs.map((job) => job.id),
      createdBatch.id,
      createdBatch.processing_status,
    );

    for (const { message } of batchJobs) message.ack();

    console.log("Created Anthropic message batch", {
      anthropicBatchId: createdBatch.id,
      processingStatus: createdBatch.processing_status,
      queueMessageCount: jobs.length,
      model: ANTHROPIC_MODEL,
    });
  } catch (error) {
    console.error("Anthropic batch creation threw an error", {
      error: asErrorMessage(error),
    });
    for (const { message } of batchJobs) {
      message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
    }
  }
}

async function processSyncMode(
  syncJobs: { message: Message<FeedbackJob>; job: FeedbackJob }[],
  env: WorkerEnv,
): Promise<void> {
  // Call Anthropic's Messages API once per job, in parallel. Simpler and
  // fast enough for an interactive demo — no cron, results land in D1 in
  // seconds instead of minutes. Each job acks/retries independently.
  await Promise.all(syncJobs.map(({ message, job }) => runSyncJob(env, message, job)));
}

async function runSyncJob(
  env: WorkerEnv,
  message: Message<FeedbackJob>,
  job: FeedbackJob,
): Promise<void> {
  try {
    const response = await fetch("https://api.anthropic.com/v1/messages", {
      method: "POST",
      headers: anthropicHeaders(env.ANTHROPIC_API_KEY),
      body: JSON.stringify(toAnthropicRequest(job).params),
    });

    if (!response.ok) {
      console.error("Anthropic messages call failed", {
        id: job.id,
        status: response.status,
        body: await response.text(),
      });
      message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
      return;
    }

    const payload = (await response.json()) as {
      content: Array<{ type: string; text?: string }>;
    };
    const text = extractAssistantText(payload.content);
    const analysis = parseFeedbackAnalysis(text);

    await markJobCompleted(env.FEEDBACK_DB, job.id, analysis);
    message.ack();

    console.log("Sync feedback job completed", {
      id: job.id,
      category: analysis.category,
      priority: analysis.priority,
    });
  } catch (error) {
    // Parse/schema errors are probably not going to fix themselves on retry,
    // so mark the row as failed and ack to keep the queue moving.
    const errorMessage = asErrorMessage(error);
    console.error("Sync feedback job failed", { id: job.id, error: errorMessage });
    await markJobFailed(env.FEEDBACK_DB, job.id, errorMessage);
    message.ack();
  }
}

function toAnthropicRequest(job: FeedbackJob): AnthropicBatchRequest {
  return {
    custom_id: job.id,
    params: {
      model: ANTHROPIC_MODEL,
      max_tokens: 120,
      system: SYSTEM_PROMPT,
      messages: [
        {
          role: "user",
          content: [
            `Feedback ID: ${job.id}`,
            `Source: ${job.source}`,
            `Submitted at: ${job.submittedAt}`,
            `Feedback text: ${job.text}`,
            "",
            "Respond with JSON only.",
            `Valid category values: ${FEEDBACK_CATEGORIES.join(", ")}.`,
            `Valid priority values: ${FEEDBACK_PRIORITIES.join(", ")}.`,
          ].join("\n"),
        },
      ],
    },
  };
}

async function insertFeedbackJob(db: D1Database, job: FeedbackJob): Promise<void> {
  await db
    .prepare(
      `INSERT INTO feedback_jobs (
        id, source, text, submitted_at, status, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'queued', ?, ?)`,
    )
    .bind(job.id, job.source, job.text, job.submittedAt, job.submittedAt, job.submittedAt)
    .run();
}

async function markEnqueueFailed(
  db: D1Database,
  id: string,
  errorText: string,
): Promise<void> {
  const now = new Date().toISOString();
  await db
    .prepare(
      `UPDATE feedback_jobs
       SET status = 'enqueue_failed',
           error_text = ?,
           updated_at = ?,
           completed_at = ?
       WHERE id = ?`,
    )
    .bind(errorText, now, now, id)
    .run();
}

async function markJobsSubmitted(
  db: D1Database,
  ids: string[],
  anthropicBatchId: string,
  processingStatus: string,
): Promise<void> {
  if (ids.length === 0) return;

  const now = new Date().toISOString();
  // D1 batch() runs all statements in a single transaction and one round-trip.
  const stmt = db.prepare(
    `UPDATE feedback_jobs
     SET status = 'anthropic_submitted',
         anthropic_batch_id = ?,
         anthropic_processing_status = ?,
         updated_at = ?
     WHERE id = ?`,
  );

  await db.batch(
    ids.map((id) => stmt.bind(anthropicBatchId, processingStatus, now, id)),
  );
}

async function markJobCompleted(
  db: D1Database,
  id: string,
  analysis: FeedbackAnalysis,
): Promise<void> {
  const now = new Date().toISOString();
  await db
    .prepare(
      `UPDATE feedback_jobs
       SET status = 'completed',
           category = ?,
           priority = ?,
           summary = ?,
           result_json = ?,
           error_text = NULL,
           updated_at = ?,
           completed_at = ?
       WHERE id = ?`,
    )
    .bind(
      analysis.category,
      analysis.priority,
      analysis.summary,
      JSON.stringify(analysis),
      now,
      now,
      id,
    )
    .run();
}

async function markJobFailed(
  db: D1Database,
  id: string,
  errorText: string,
): Promise<void> {
  const now = new Date().toISOString();
  await db
    .prepare(
      `UPDATE feedback_jobs
       SET status = 'failed',
           error_text = ?,
           updated_at = ?,
           completed_at = ?
       WHERE id = ?`,
    )
    .bind(errorText, now, now, id)
    .run();
}

async function getInProgressBatchIds(db: D1Database): Promise<string[]> {
  const result = await db
    .prepare(
      `SELECT DISTINCT anthropic_batch_id
       FROM feedback_jobs
       WHERE anthropic_batch_id IS NOT NULL
         AND anthropic_processing_status = 'in_progress'
       ORDER BY updated_at ASC
       LIMIT 25`,
    )
    .all<{ anthropic_batch_id: string }>();

  return result.results.map((row) => row.anthropic_batch_id);
}

async function pollAnthropicBatch(env: WorkerEnv, batchId: string): Promise<void> {
  try {
    const retrieveResponse = await fetch(`${ANTHROPIC_API_URL}/${batchId}`, {
      headers: anthropicHeaders(env.ANTHROPIC_API_KEY),
    });

    if (!retrieveResponse.ok) {
      console.error("Failed to retrieve Anthropic batch", {
        batchId,
        status: retrieveResponse.status,
        body: await retrieveResponse.text(),
      });
      return;
    }

    const retrievedBatch =
      (await retrieveResponse.json()) as AnthropicBatchRetrieveResponse;

    await updateBatchProcessingStatus(
      env.FEEDBACK_DB,
      batchId,
      retrievedBatch.processing_status,
    );

    console.log("Polled Anthropic batch", {
      batchId,
      processingStatus: retrievedBatch.processing_status,
      requestCounts: retrievedBatch.request_counts,
    });

    if (retrievedBatch.processing_status !== "ended" || !retrievedBatch.results_url) {
      return;
    }

    const resultsResponse = await fetch(retrievedBatch.results_url, {
      headers: anthropicHeaders(env.ANTHROPIC_API_KEY),
    });

    if (!resultsResponse.ok) {
      console.error("Failed to retrieve Anthropic batch results", {
        batchId,
        status: resultsResponse.status,
        body: await resultsResponse.text(),
      });
      return;
    }

    const resultsText = await resultsResponse.text();
    const lines = resultsText
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean);

    const resultLines = lines.map(
      (line) => JSON.parse(line) as AnthropicBatchResultLine,
    );

    await storeBatchResults(env.FEEDBACK_DB, resultLines);

    console.log("Stored Anthropic batch results", {
      batchId,
      resultCount: resultLines.length,
    });
  } catch (error) {
    console.error("Cron poll failed for Anthropic batch", {
      batchId,
      error: asErrorMessage(error),
    });
  }
}

async function updateBatchProcessingStatus(
  db: D1Database,
  batchId: string,
  processingStatus: string,
): Promise<void> {
  await db
    .prepare(
      `UPDATE feedback_jobs
       SET anthropic_processing_status = ?, updated_at = ?
       WHERE anthropic_batch_id = ?`,
    )
    .bind(processingStatus, new Date().toISOString(), batchId)
    .run();
}

async function storeBatchResults(
  db: D1Database,
  resultLines: AnthropicBatchResultLine[],
): Promise<void> {
  if (resultLines.length === 0) return;

  const now = new Date().toISOString();
  const statements: D1PreparedStatement[] = [];

  // Prepare one UPDATE per result; flush them all in a single D1 transaction.
  // Terminal rows also get `completed_at` stamped so the API can report elapsed time.
  const completedStmt = db.prepare(
    `UPDATE feedback_jobs
     SET status = 'completed',
         anthropic_processing_status = 'ended',
         category = ?,
         priority = ?,
         summary = ?,
         result_json = ?,
         error_text = NULL,
         updated_at = ?,
         completed_at = ?
     WHERE id = ?`,
  );
  const failedStmt = db.prepare(
    `UPDATE feedback_jobs
     SET status = 'failed',
         anthropic_processing_status = 'ended',
         error_text = ?,
         updated_at = ?,
         completed_at = ?
     WHERE id = ?`,
  );
  const canceledStmt = db.prepare(
    `UPDATE feedback_jobs
     SET status = 'canceled',
         anthropic_processing_status = 'ended',
         updated_at = ?,
         completed_at = ?
     WHERE id = ?`,
  );
  const expiredStmt = db.prepare(
    `UPDATE feedback_jobs
     SET status = 'expired',
         anthropic_processing_status = 'ended',
         updated_at = ?,
         completed_at = ?
     WHERE id = ?`,
  );

  for (const line of resultLines) {
    switch (line.result.type) {
      case "succeeded": {
        try {
          const text = extractAssistantText(line.result.message.content);
          const analysis = parseFeedbackAnalysis(text);
          statements.push(
            completedStmt.bind(
              analysis.category,
              analysis.priority,
              analysis.summary,
              JSON.stringify(analysis),
              now,
              now,
              line.custom_id,
            ),
          );
        } catch (error) {
          statements.push(
            failedStmt.bind(asErrorMessage(error), now, now, line.custom_id),
          );
        }
        break;
      }
      case "errored": {
        const message =
          line.result.error?.error?.message ?? "Anthropic batch item failed";
        statements.push(failedStmt.bind(message, now, now, line.custom_id));
        break;
      }
      case "canceled": {
        statements.push(canceledStmt.bind(now, now, line.custom_id));
        break;
      }
      case "expired": {
        statements.push(expiredStmt.bind(now, now, line.custom_id));
        break;
      }
    }
  }

  await db.batch(statements);
}

async function getFeedbackRow(db: D1Database, id: string): Promise<FeedbackRow | null> {
  const row = await db
    .prepare(
      `SELECT
         id,
         source,
         text,
         submitted_at,
         status,
         anthropic_batch_id,
         anthropic_processing_status,
         category,
         priority,
         summary,
         result_json,
         error_text,
         created_at,
         updated_at,
         completed_at
       FROM feedback_jobs
       WHERE id = ?`,
    )
    .bind(id)
    .first<FeedbackRow>();

  return row ?? null;
}

function normalizeFeedbackRow(row: FeedbackRow) {
  // `elapsed_ms` always measures from submission; it keeps ticking until the
  // row reaches a terminal status (completed / failed / canceled / expired /
  // enqueue_failed), at which point it freezes at the real processing time.
  const submittedMs = Date.parse(row.submitted_at);
  const endMs = row.completed_at ? Date.parse(row.completed_at) : Date.now();
  const elapsedMs =
    Number.isFinite(submittedMs) && Number.isFinite(endMs)
      ? Math.max(0, endMs - submittedMs)
      : null;

  return {
    ...row,
    result: row.result_json ? safeJsonParse(row.result_json) : null,
    elapsed_ms: elapsedMs,
  };
}

function extractAssistantText(
  content: Array<{
    type: string;
    text?: string;
  }>,
): string {
  return content
    .filter((block) => block.type === "text" && typeof block.text === "string")
    .map((block) => block.text)
    .join("\n")
    .trim();
}

function parseFeedbackAnalysis(text: string): FeedbackAnalysis {
  // Claude occasionally wraps JSON in ```json fences or adds a stray trailing
  // comma. `jsonrepair` tolerates both without brittle regex gymnastics.
  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonrepair(text));
  } catch (error) {
    throw new Error(`Could not parse Anthropic response as JSON: ${asErrorMessage(error)}`);
  }

  if (
    parsed &&
    typeof parsed === "object" &&
    "category" in parsed &&
    "priority" in parsed &&
    "summary" in parsed &&
    typeof (parsed as Record<string, unknown>).category === "string" &&
    typeof (parsed as Record<string, unknown>).priority === "string" &&
    typeof (parsed as Record<string, unknown>).summary === "string"
  ) {
    const candidate = parsed as {
      category: string;
      priority: string;
      summary: string;
    };

    if (
      (FEEDBACK_CATEGORIES as readonly string[]).includes(candidate.category) &&
      (FEEDBACK_PRIORITIES as readonly string[]).includes(candidate.priority)
    ) {
      return {
        category: candidate.category as FeedbackCategory,
        priority: candidate.priority as FeedbackPriority,
        summary: candidate.summary.trim(),
      };
    }
  }

  throw new Error("Anthropic result did not match the expected JSON schema");
}

function anthropicHeaders(apiKey: string): HeadersInit {
  return {
    "content-type": "application/json",
    "x-api-key": apiKey,
    "anthropic-version": ANTHROPIC_API_VERSION,
  };
}

function asNonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function asFeedbackSource(value: unknown): FeedbackSource | null {
  return typeof value === "string" &&
    (FEEDBACK_SOURCES as readonly string[]).includes(value)
    ? (value as FeedbackSource)
    : null;
}

function asFeedbackMode(value: unknown): FeedbackMode | null {
  return typeof value === "string" &&
    (FEEDBACK_MODES as readonly string[]).includes(value)
    ? (value as FeedbackMode)
    : null;
}

function asFeedbackJob(value: unknown): FeedbackJob {
  if (
    value &&
    typeof value === "object" &&
    "id" in value &&
    "source" in value &&
    "text" in value &&
    "submittedAt" in value
  ) {
    const candidate = value as Record<string, unknown>;
    if (
      typeof candidate.id === "string" &&
      typeof candidate.source === "string" &&
      typeof candidate.text === "string" &&
      typeof candidate.submittedAt === "string" &&
      (FEEDBACK_SOURCES as readonly string[]).includes(candidate.source)
    ) {
      return {
        id: candidate.id,
        source: candidate.source as FeedbackSource,
        text: candidate.text,
        submittedAt: candidate.submittedAt,
        // Default to `batch` for backward compatibility with any in-flight
        // messages enqueued before this field existed.
        mode: asFeedbackMode(candidate.mode) ?? "batch",
      };
    }
  }

  throw new Error("Queue message body is not a valid FeedbackJob");
}

function asErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}

function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
    },
  });
}

function html(markup: string, status = 200): Response {
  return new Response(markup, {
    status,
    headers: {
      "content-type": "text/html; charset=utf-8",
    },
  });
}

function renderDemoPage(): string {
  const terminalStatuses = JSON.stringify(TERMINAL_STATUSES);
  const sourceOptions = FEEDBACK_SOURCES.map(
    (source) => `<option value="${source}">${source}</option>`,
  ).join("");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Feedback Demo</title>
    <style>
      :root {
        color-scheme: light;
        font-family: ui-sans-serif, system-ui, sans-serif;
        --bg: #f6f4ef;
        --panel: rgba(255, 255, 255, 0.86);
        --panel-border: #d8d1c5;
        --surface: #fffdf9;
        --surface-muted: #f3eee6;
        --text: #1f1d1a;
        --text-muted: #5e584f;
        --accent: #1f1d1a;
        --accent-contrast: #f8f4ec;
        --focus: #b9a88a;
        --radius-lg: 18px;
        --radius-md: 14px;
        --radius-pill: 999px;
        background: var(--bg);
        color: var(--text);
      }
      body {
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background:
          radial-gradient(circle at top, rgba(186, 173, 146, 0.28), transparent 36%),
          var(--bg);
      }
      main {
        width: min(92vw, 460px);
        padding: 24px;
        border: 1px solid var(--panel-border);
        border-radius: var(--radius-lg);
        background: var(--panel);
        box-shadow: 0 18px 40px rgba(44, 38, 28, 0.08);
      }
      h1 {
        margin: 0 0 8px;
        font-size: 1.4rem;
      }
      p {
        margin: 0 0 18px;
        color: var(--text-muted);
      }
      form {
        display: grid;
        gap: 12px;
      }
      input, textarea, button {
        font: inherit;
      }
      select, textarea {
        width: 100%;
        box-sizing: border-box;
        border: 1px solid var(--panel-border);
        border-radius: var(--radius-md);
        padding: 12px 14px;
        background: var(--surface);
        color: var(--text);
        transition: border-color 120ms ease, box-shadow 120ms ease, background 120ms ease;
      }
      select {
        appearance: none;
        background:
          linear-gradient(45deg, transparent 50%, var(--text-muted) 50%) calc(100% - 22px) calc(50% - 2px) / 8px 8px no-repeat,
          linear-gradient(135deg, var(--text-muted) 50%, transparent 50%) calc(100% - 16px) calc(50% - 2px) / 8px 8px no-repeat,
          var(--surface);
        padding-right: 38px;
      }
      textarea {
        min-height: 140px;
        resize: vertical;
      }
      select:focus, textarea:focus {
        outline: none;
        border-color: var(--focus);
        box-shadow: 0 0 0 3px rgba(185, 168, 138, 0.18);
        background: #fffefa;
      }
      button {
        border: 0;
        border-radius: var(--radius-pill);
        padding: 12px 16px;
        background: var(--accent);
        color: var(--accent-contrast);
        cursor: pointer;
        transition: opacity 120ms ease, transform 120ms ease;
      }
      button:hover {
        opacity: 0.96;
      }
      button:active {
        transform: translateY(1px);
      }
      pre {
        margin: 14px 0 0;
        padding: 12px;
        border-radius: var(--radius-md);
        border: 1px solid rgba(31, 29, 26, 0.08);
        background: var(--accent);
        color: var(--accent-contrast);
        overflow: auto;
        white-space: pre-wrap;
      }
      .hint {
        margin-top: 12px;
        font-size: 0.92rem;
        color: var(--text-muted);
      }
      .mode {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 8px;
        padding: 4px;
        border: 1px solid var(--panel-border);
        border-radius: var(--radius-pill);
        background: var(--surface-muted);
      }
      .mode label {
        position: relative;
        display: grid;
        place-items: center;
        padding: 10px 12px;
        border-radius: var(--radius-pill);
        font-size: 0.95rem;
        color: var(--text-muted);
        cursor: pointer;
        user-select: none;
        transition: background 120ms ease, color 120ms ease;
      }
      .mode label small {
        display: block;
        font-size: 0.72rem;
        opacity: 0.7;
        margin-top: 2px;
      }
      .mode input {
        position: absolute;
        opacity: 0;
        pointer-events: none;
      }
      .mode input:checked + span {
        color: var(--accent-contrast);
      }
      .mode label:has(input:checked) {
        background: var(--accent);
        color: var(--accent-contrast);
      }
    </style>
  </head>
  <body>
    <main>
      <h1>Feedback Demo</h1>
      <p>Submit feedback, persist it in D1, queue it, and let Anthropic classify it.</p>
      <form id="feedback-form">
        <select name="source" required>
          ${sourceOptions}
        </select>
        <div class="mode" role="radiogroup" aria-label="Processing mode">
          <label>
            <input type="radio" name="mode" value="sync" checked />
            <span>Sync<small>Messages API &middot; seconds</small></span>
          </label>
          <label>
            <input type="radio" name="mode" value="batch" />
            <span>Batch<small>50% cheaper &middot; minutes</small></span>
          </label>
        </div>
        <textarea name="text" placeholder="What did the user say?" required></textarea>
        <button type="submit">Queue Feedback</button>
      </form>
      <p class="hint">Sync calls Anthropic once per job. Batch waits for 3 messages (or 60s) and submits them together via the Message Batches API.</p>
      <p id="status" class="hint"></p>
      <pre id="output">Waiting for submission...</pre>
    </main>
    <script>
      const TERMINAL_STATUSES = ${terminalStatuses};
      const POLL_INTERVAL_MS = 2000;
      const POLL_TIMEOUT_MS = 10 * 60 * 1000; // Anthropic batches usually finish <10m.

      const form = document.getElementById("feedback-form");
      const output = document.getElementById("output");
      const status = document.getElementById("status");
      let pollTimer = null;

      function stopPolling() {
        if (pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
      }

      function formatElapsed(ms) {
        if (ms == null) return "";
        const totalSeconds = Math.floor(ms / 1000);
        const m = Math.floor(totalSeconds / 60);
        const s = totalSeconds % 60;
        return m > 0 ? m + "m " + s + "s" : s + "s";
      }

      async function pollFeedback(id) {
        const response = await fetch("/api/feedback/" + id);
        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);
        const feedback = body.feedback || {};
        const isTerminal = TERMINAL_STATUSES.includes(feedback.status);
        const elapsed = formatElapsed(feedback.elapsed_ms);
        status.textContent =
          (isTerminal ? "Done in " : "Elapsed: ") + elapsed + " \u2022 status: " + feedback.status;
        return isTerminal;
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        stopPolling();

        const formData = new FormData(form);
        const payload = {
          source: formData.get("source"),
          text: formData.get("text"),
          mode: formData.get("mode"),
        };

        output.textContent = "Submitting...";
        status.textContent = "";

        const response = await fetch("/feedback", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        });

        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);

        if (!body.ok || !body.id) return;

        const startedAt = Date.now();
        pollTimer = setInterval(async () => {
          const done = await pollFeedback(body.id);
          if (done) {
            stopPolling();
            return;
          }
          if (Date.now() - startedAt > POLL_TIMEOUT_MS) {
            stopPolling();
            status.textContent =
              "Stopped polling after " + formatElapsed(POLL_TIMEOUT_MS) +
              " \u2014 check Anthropic dashboard.";
          }
        }, POLL_INTERVAL_MS);
      });
    </script>
  </body>
</html>`;
}
