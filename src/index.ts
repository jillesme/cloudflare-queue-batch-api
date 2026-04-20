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
const TERMINAL_STATUSES = ["completed", "failed", "enqueue_failed"] as const;

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

// Anthropic sends `type: "succeeded" | "errored" | "canceled" | "expired"`.
// For a demo we only care about the happy path; anything else is a failure.
interface AnthropicBatchResultLine {
  custom_id: string;
  result: {
    type: "succeeded" | "errored" | "canceled" | "expired";
    message?: {
      content: Array<{ type: string; text?: string }>;
    };
    error?: {
      error?: { message?: string };
    };
  };
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

    // Split by mode. Batch-mode jobs become one Anthropic Message Batch;
    // sync-mode jobs fan out to individual /v1/messages calls in parallel.
    const batchModeJobs = batch.messages.filter((m) => m.body.mode === "batch");
    const syncModeJobs = batch.messages.filter((m) => m.body.mode === "sync");

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

  const text = typeof body.text === "string" ? body.text.trim() : "";
  const source = body.source as FeedbackSource;
  const mode: FeedbackMode =
    body.mode === "sync" || body.mode === "batch" ? body.mode : "batch";

  if (!text || !FEEDBACK_SOURCES.includes(source)) {
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
    text: previewText(job.text),
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
  messages: Message<FeedbackJob>[],
  env: WorkerEnv,
): Promise<void> {
  // Turn the queue batch into one Anthropic Message Batch request.
  // Cheaper (50% discount) but asynchronous — the cron handler picks up results later.
  const jobs = messages.map((m) => m.body);
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
      for (const message of messages) {
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

    for (const message of messages) message.ack();

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
    for (const message of messages) {
      message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
    }
  }
}

async function processSyncMode(
  messages: Message<FeedbackJob>[],
  env: WorkerEnv,
): Promise<void> {
  // Call Anthropic's Messages API once per job, in parallel. Simpler and
  // fast enough for an interactive demo — no cron, results land in D1 in
  // seconds instead of minutes. Each job acks/retries independently.
  await Promise.all(messages.map((message) => runSyncJob(env, message)));
}

async function runSyncJob(
  env: WorkerEnv,
  message: Message<FeedbackJob>,
): Promise<void> {
  const job = message.body;
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
      source: job.source,
      text: previewText(job.text),
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
  // Track per-item outcomes so we can log them after the batch commits. The
  // demo narration benefits from seeing each feedback mapped to its category.
  const outcomes: Array<{
    id: string;
    status: "completed" | "failed";
    category?: string;
    priority?: string;
    error?: string;
  }> = [];

  // Two prepared UPDATEs are enough: happy path writes the analysis fields;
  // any non-succeeded result (errored/canceled/expired) is recorded as failed.
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

  for (const line of resultLines) {
    if (line.result.type === "succeeded" && line.result.message) {
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
        outcomes.push({
          id: line.custom_id,
          status: "completed",
          category: analysis.category,
          priority: analysis.priority,
        });
        continue;
      } catch (error) {
        const errorMessage = asErrorMessage(error);
        statements.push(failedStmt.bind(errorMessage, now, now, line.custom_id));
        outcomes.push({ id: line.custom_id, status: "failed", error: errorMessage });
        continue;
      }
    }

    const errorMessage =
      line.result.error?.error?.message ?? `Anthropic batch item ${line.result.type}`;
    statements.push(failedStmt.bind(errorMessage, now, now, line.custom_id));
    outcomes.push({ id: line.custom_id, status: "failed", error: errorMessage });
  }

  await db.batch(statements);

  if (outcomes.length === 0) return;

  // Pull back source/text so the demo log shows which feedback became which
  // classification. Using an IN() list keeps this to a single D1 read.
  const placeholders = outcomes.map(() => "?").join(", ");
  const rows = await db
    .prepare(
      `SELECT id, source, text FROM feedback_jobs WHERE id IN (${placeholders})`,
    )
    .bind(...outcomes.map((o) => o.id))
    .all<{ id: string; source: string; text: string }>();

  const byId = new Map(rows.results.map((row) => [row.id, row]));
  for (const outcome of outcomes) {
    const row = byId.get(outcome.id);
    console.log("Batch feedback job " + outcome.status, {
      id: outcome.id,
      source: row?.source,
      text: row ? previewText(row.text) : undefined,
      category: outcome.category,
      priority: outcome.priority,
      error: outcome.error,
    });
  }
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

function asErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error";
}

// Trim feedback text to a short single-line preview so console logs stay
// readable while still showing what the model was actually classifying.
function previewText(text: string, max = 60): string {
  const collapsed = text.replace(/\s+/g, " ").trim();
  return collapsed.length <= max ? collapsed : collapsed.slice(0, max - 1) + "…";
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
    <title>Feedback Classifier · Cloudflare Demo</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link
      href="https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=JetBrains+Mono:wght@400;500&display=swap"
      rel="stylesheet"
    />
    <style>
      :root {
        color-scheme: light;
        --cream: #FFFCF6;
        --coffee: #2C1A14;
        --orange: #E85E2E;
        --taupe: #DCCDBF;
        --white: #FFFFFF;
        --coffee-60: rgba(44, 26, 20, 0.6);
        --coffee-40: rgba(44, 26, 20, 0.4);
      }
      * { box-sizing: border-box; }
      html, body { margin: 0; padding: 0; }
      body {
        background: var(--cream);
        color: var(--coffee);
        font-family: "Geist", ui-sans-serif, system-ui, sans-serif;
        font-feature-settings: "ss01", "cv11";
        -webkit-font-smoothing: antialiased;
      }
      main {
        width: 100%;
        max-width: 520px;
        margin: 0 auto;
        padding: 24px;
        display: flex;
        flex-direction: column;
        gap: 16px;
      }
      h1 {
        margin: 0;
        font-family: "Geist", sans-serif;
        font-weight: 500;
        font-size: 18px;
        letter-spacing: -0.01em;
        line-height: 1.2;
        display: flex;
        align-items: center;
        gap: 10px;
      }
      h1::before {
        content: "";
        width: 14px;
        height: 2px;
        background: var(--orange);
      }
      form { display: grid; gap: 14px; }
      .field { display: grid; gap: 6px; }
      label.field-label {
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 11px;
        font-weight: 500;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--coffee-60);
      }
      select, textarea {
        font: inherit;
        width: 100%;
        border: 0;
        border-bottom: 1px solid var(--taupe);
        background: transparent;
        color: var(--coffee);
        padding: 10px 0;
        border-radius: 0;
        transition: border-color 140ms ease;
      }
      select {
        appearance: none;
        background-image: linear-gradient(45deg, transparent 50%, var(--coffee) 50%),
                          linear-gradient(135deg, var(--coffee) 50%, transparent 50%);
        background-position: calc(100% - 14px) 18px, calc(100% - 8px) 18px;
        background-size: 6px 6px, 6px 6px;
        background-repeat: no-repeat;
        padding-right: 28px;
        cursor: pointer;
      }
      textarea {
        min-height: 56px;
        resize: vertical;
        line-height: 1.5;
      }
      select:focus-visible, textarea:focus-visible {
        outline: none;
        border-bottom-color: var(--orange);
      }
      ::placeholder { color: var(--coffee-40); }

      .mode {
        display: grid;
        grid-template-columns: 1fr 1fr;
        border: 1px solid var(--taupe);
        background: var(--white);
      }
      .mode label {
        position: relative;
        display: grid;
        padding: 10px 14px;
        gap: 2px;
        cursor: pointer;
        user-select: none;
        transition: background 140ms ease, color 140ms ease;
      }
      .mode label + label { border-left: 1px solid var(--taupe); }
      .mode input {
        position: absolute;
        opacity: 0;
        pointer-events: none;
      }
      .mode .title {
        font-weight: 500;
        font-size: 14px;
        letter-spacing: -0.005em;
      }
      .mode .caption {
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 11px;
        color: var(--coffee-60);
        letter-spacing: 0.02em;
      }
      .mode label:has(input:checked) {
        background: var(--orange);
        color: var(--white);
      }
      .mode label:has(input:checked) .caption { color: rgba(255, 255, 255, 0.8); }

      button[type="submit"] {
        font: inherit;
        font-weight: 500;
        font-size: 14px;
        letter-spacing: 0.02em;
        padding: 10px 18px;
        border: 0;
        background: var(--orange);
        color: var(--white);
        cursor: pointer;
        transition: background 140ms ease, transform 80ms ease;
        justify-self: start;
        display: inline-flex;
        align-items: center;
        gap: 10px;
      }
      button[type="submit"]::after {
        content: "→";
        font-family: "JetBrains Mono", ui-monospace, monospace;
      }
      button[type="submit"]:hover { background: #D54F20; }
      button[type="submit"]:active { transform: translateY(1px); }
      button[type="submit"]:focus-visible {
        outline: 2px solid var(--coffee);
        outline-offset: 3px;
      }

      .divider {
        height: 1px;
        background: var(--taupe);
        margin: 0;
      }

      .status {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 12px;
        color: var(--coffee-60);
        letter-spacing: 0.02em;
        min-height: 18px;
      }
      .status .elapsed { font-variant-numeric: tabular-nums; color: var(--coffee); }
      .status .dot {
        width: 6px; height: 6px;
        border-radius: 50%;
        background: var(--coffee-40);
        margin-right: 8px;
        display: inline-block;
        vertical-align: middle;
      }
      .status[data-state="active"] .dot { background: var(--orange); animation: pulse 1.2s ease-in-out infinite; }
      .status[data-state="done"] .dot { background: #3FA66B; }
      @keyframes pulse {
        0%, 100% { opacity: 0.35; }
        50% { opacity: 1; }
      }

      .output {
        border: 1px solid var(--taupe);
        background: var(--white);
        padding: 12px 14px;
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 11.5px;
        line-height: 1.5;
        color: var(--coffee);
        white-space: pre-wrap;
        overflow-x: auto;
        max-height: 160px;
        overflow-y: auto;
        margin: 0;
      }
      .output-label {
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 11px;
        font-weight: 500;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--coffee-60);
        margin-bottom: 10px;
      }

      footer {
        font-family: "JetBrains Mono", ui-monospace, monospace;
        font-size: 11px;
        color: var(--coffee-40);
        letter-spacing: 0.04em;
      }
      footer a {
        color: var(--orange);
        text-decoration: none;
        border-bottom: 1px solid transparent;
      }
      footer a:hover { border-bottom-color: var(--orange); }
    </style>
  </head>
  <body>
    <main>
      <h1>Cloudflare Queues Demo</h1>

      <form id="feedback-form">
        <div class="field">
          <label class="field-label" for="source">Source</label>
          <select id="source" name="source" required>
            ${sourceOptions}
          </select>
        </div>

        <div class="field">
          <span class="field-label">Processing mode</span>
          <div class="mode" role="radiogroup" aria-label="Processing mode">
            <label>
              <input type="radio" name="mode" value="sync" checked />
              <span class="title">Sync</span>
              <span class="caption">Messages API &middot; seconds</span>
            </label>
            <label>
              <input type="radio" name="mode" value="batch" />
              <span class="title">Batch</span>
              <span class="caption">50% cheaper &middot; minutes</span>
            </label>
          </div>
        </div>

        <div class="field">
          <label class="field-label" for="feedback-text">Feedback</label>
          <textarea id="feedback-text" name="text" placeholder="What did the user say?" required></textarea>
        </div>

        <button type="submit">Classify feedback</button>
      </form>

      <section>
        <div class="status" id="status" data-state="idle">
          <span><span class="dot"></span><span id="status-text">Awaiting submission</span></span>
          <span class="elapsed" id="status-elapsed"></span>
        </div>
        <pre class="output" id="output" style="margin-top: 8px;">{ "ready": true }</pre>
      </section>

      <footer>
        queue &rarr; anthropic &rarr; d1 &nbsp;&middot;&nbsp;
        <a href="https://developers.cloudflare.com/queues/" target="_blank" rel="noreferrer">docs</a>
      </footer>
    </main>
    <script>
      const TERMINAL_STATUSES = ${terminalStatuses};
      // Demo polls a handful of times then stops — this is a demo, not a
      // long-lived dashboard. Batch jobs can take minutes; the user can refresh
      // the page or hit the API directly once the cron has caught up.
      const POLL_SCHEDULE_MS = [1000, 1500, 2000, 3000, 5000, 8000, 13000];

      const form = document.getElementById("feedback-form");
      const output = document.getElementById("output");
      const statusRoot = document.getElementById("status");
      const statusText = document.getElementById("status-text");
      const statusElapsed = document.getElementById("status-elapsed");
      let pollTimer = null;
      let pollToken = 0;

      function setStatus(state, text, elapsed) {
        statusRoot.dataset.state = state;
        statusText.textContent = text;
        statusElapsed.textContent = elapsed || "";
      }

      function stopPolling() {
        pollToken++;
        if (pollTimer) {
          clearTimeout(pollTimer);
          pollTimer = null;
        }
      }

      function formatElapsed(ms) {
        if (ms == null) return "";
        const totalSeconds = Math.floor(ms / 1000);
        const m = Math.floor(totalSeconds / 60);
        const s = totalSeconds % 60;
        const mm = String(m).padStart(2, "0");
        const ss = String(s).padStart(2, "0");
        return mm + ":" + ss;
      }

      async function pollFeedback(id) {
        const response = await fetch("/api/feedback/" + id);
        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);
        const feedback = body.feedback || {};
        const isTerminal = TERMINAL_STATUSES.includes(feedback.status);
        const elapsed = formatElapsed(feedback.elapsed_ms);
        setStatus(
          isTerminal ? "done" : "active",
          feedback.status || "queued",
          elapsed,
        );
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

        output.textContent = "// submitting…";
        setStatus("active", "submitting", "");

        const response = await fetch("/feedback", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(payload),
        });

        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);

        if (!body.ok || !body.id) {
          setStatus("idle", "error", "");
          return;
        }

        setStatus("active", body.status || "queued", "00:00");
        const token = ++pollToken;
        let attempt = 0;

        const schedule = () => {
          const delay =
            POLL_SCHEDULE_MS[Math.min(attempt, POLL_SCHEDULE_MS.length - 1)];
          pollTimer = setTimeout(async () => {
            if (token !== pollToken) return;
            const done = await pollFeedback(body.id);
            if (token !== pollToken) return;
            if (done) {
              pollTimer = null;
              return;
            }
            attempt++;
            if (attempt >= POLL_SCHEDULE_MS.length) {
              pollTimer = null;
              setStatus("idle", "still working — refresh to check", "");
              return;
            }
            schedule();
          }, delay);
        };
        schedule();
      });
    </script>
  </body>
</html>`;
}
