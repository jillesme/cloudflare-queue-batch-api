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
const TERMINAL_STATUSES = new Set([
  "completed",
  "failed",
  "canceled",
  "expired",
  "enqueue_failed",
]);

type FeedbackCategory = (typeof FEEDBACK_CATEGORIES)[number];
type FeedbackSource = (typeof FEEDBACK_SOURCES)[number];
type FeedbackPriority = (typeof FEEDBACK_PRIORITIES)[number];

interface FeedbackJob {
  id: string;
  source: FeedbackSource;
  text: string;
  submittedAt: string;
}

interface FeedbackRequestBody {
  source?: unknown;
  text?: unknown;
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
  feedback_analysis_db: D1Database;
}

const ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages/batches";
const ANTHROPIC_API_VERSION = "2023-06-01";
const ANTHROPIC_MODEL = "claude-haiku-4-5";
const SYSTEM_PROMPT = [
  "You analyze user feedback for a product team.",
  "Classify the feedback into exactly one category: bug, feature_request, praise, billing, or other.",
  "Assign exactly one priority: low, medium, or high.",
  "Return compact JSON with keys category, priority, and summary.",
  "The summary must be a short sentence under 20 words.",
].join(" ");

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (request.method === "GET" && pathname === "/") {
      return html(renderDemoPage());
    }

    if (request.method === "GET" && pathname.startsWith("/api/feedback/")) {
      const id = pathname.replace("/api/feedback/", "").trim();
      const row = id ? await getFeedbackRow(env.feedback_analysis_db, id) : null;

      if (!row) {
        return json({ ok: false, error: "Feedback item not found" }, 404);
      }

      return json({ ok: true, feedback: normalizeFeedbackRow(row) });
    }

    if (request.method !== "POST" || pathname !== "/feedback") {
      return json(
        {
          ok: false,
          error:
            "Use GET / for the demo form, GET /api/feedback/:id for status, or POST /feedback with JSON: { source, text }",
        },
        404,
      );
    }

    let body: FeedbackRequestBody;

    try {
      body = (await request.json()) as FeedbackRequestBody;
    } catch {
      return json({ ok: false, error: "Invalid JSON body" }, 400);
    }

    const source = asFeedbackSource(body.source);
    const text = asNonEmptyString(body.text);

    if (!source || !text) {
      return json(
        {
          ok: false,
          error: "`source` must be one of the allowed options and `text` must be a non-empty string",
        },
        400,
      );
    }

    const job: FeedbackJob = {
      id: crypto.randomUUID(),
      source,
      text,
      submittedAt: new Date().toISOString(),
    };

    await insertFeedbackJob(env.feedback_analysis_db, job);

    console.log("Received feedback submission", {
      id: job.id,
      source: job.source,
      textLength: job.text.length,
    });

    try {
      // Producer: accept user feedback over HTTP, persist it to D1, then enqueue
      // a typed job for async queue-based processing.
      await env.FEEDBACK_QUEUE.send(job);

      console.log("Enqueued feedback job", {
        id: job.id,
        source: job.source,
      });

      return json({ ok: true, status: "queued", id: job.id });
    } catch (error) {
      await markEnqueueFailed(env.feedback_analysis_db, job.id, asErrorMessage(error));
      console.error("Failed to enqueue feedback job", { id: job.id, error });
      return json({ ok: false, error: "Failed to enqueue feedback" }, 500);
    }
  },

  async queue(batch: MessageBatch<unknown>, env: WorkerEnv): Promise<void> {
    console.log("Queue consumer received batch", {
      messageCount: batch.messages.length,
      ids: batch.messages.map((message) => message.id),
    });

    const jobs = batch.messages.map((message) => asFeedbackJob(message.body));

    // Consumer: Cloudflare delivers multiple queue messages together based on
    // max_batch_size / max_batch_timeout from wrangler.jsonc.
    const requests: AnthropicBatchRequest[] = jobs.map(toAnthropicRequest);

    try {
      // Anthropic Message Batches expects a top-level `requests` array where each
      // item includes `custom_id` and `params` for one Messages API request.
      const response = await fetch(ANTHROPIC_API_URL, {
        method: "POST",
        headers: anthropicHeaders(env.ANTHROPIC_API_KEY),
        body: JSON.stringify({ requests }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error("Anthropic batch creation failed", {
          status: response.status,
          body: errorText,
        });
        batch.retryAll({ delaySeconds: 600 });
        return;
      }

      const createdBatch =
        (await response.json()) as AnthropicBatchCreateResponse;

      await markJobsSubmitted(
        env.feedback_analysis_db,
        jobs.map((job) => job.id),
        createdBatch.id,
        createdBatch.processing_status,
      );

      console.log("Created Anthropic message batch", {
        anthropicBatchId: createdBatch.id,
        processingStatus: createdBatch.processing_status,
        queueMessageCount: batch.messages.length,
        model: ANTHROPIC_MODEL,
      });
    } catch (error) {
      console.error("Anthropic batch creation threw an error", error);
      batch.retryAll({ delaySeconds: 600 });
    }
  },

  async scheduled(
    controller: ScheduledController,
    env: WorkerEnv,
  ): Promise<void> {
    console.log("Cron poll started", {
      cron: controller.cron,
      scheduledTime: new Date(controller.scheduledTime).toISOString(),
    });

    const batchIds = await getInProgressBatchIds(env.feedback_analysis_db);

    if (batchIds.length === 0) {
      console.log("Cron poll found no in-progress Anthropic batches");
      return;
    }

    console.log("Cron poll checking Anthropic batches", { batchIds });

    for (const batchId of batchIds) {
      await pollAnthropicBatch(env, batchId);
    }
  },
} satisfies ExportedHandler<WorkerEnv>;

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
  await db
    .prepare(
      `UPDATE feedback_jobs
       SET status = 'enqueue_failed', error_text = ?, updated_at = ?
       WHERE id = ?`,
    )
    .bind(errorText, new Date().toISOString(), id)
    .run();
}

async function markJobsSubmitted(
  db: D1Database,
  ids: string[],
  anthropicBatchId: string,
  processingStatus: string,
): Promise<void> {
  const now = new Date().toISOString();

  for (const id of ids) {
    await db
      .prepare(
        `UPDATE feedback_jobs
         SET status = 'anthropic_submitted',
             anthropic_batch_id = ?,
             anthropic_processing_status = ?,
             updated_at = ?
         WHERE id = ?`,
      )
      .bind(anthropicBatchId, processingStatus, now, id)
      .run();
  }
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
      env.feedback_analysis_db,
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

    for (const line of lines) {
      const resultLine = JSON.parse(line) as AnthropicBatchResultLine;
      await storeBatchResult(env.feedback_analysis_db, resultLine);
    }

    console.log("Stored Anthropic batch results", {
      batchId,
      resultCount: lines.length,
    });
  } catch (error) {
    console.error("Cron poll failed for Anthropic batch", { batchId, error });
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

async function storeBatchResult(
  db: D1Database,
  resultLine: AnthropicBatchResultLine,
): Promise<void> {
  const now = new Date().toISOString();

  if (resultLine.result.type === "succeeded") {
    try {
      const text = extractAssistantText(resultLine.result.message.content);
      const analysis = parseFeedbackAnalysis(text);

      await db
        .prepare(
          `UPDATE feedback_jobs
           SET status = 'completed',
               anthropic_processing_status = 'ended',
               category = ?,
               priority = ?,
               summary = ?,
               result_json = ?,
               error_text = NULL,
               updated_at = ?
           WHERE id = ?`,
        )
        .bind(
          analysis.category,
          analysis.priority,
          analysis.summary,
          JSON.stringify(analysis),
          now,
          resultLine.custom_id,
        )
        .run();
    } catch (error) {
      await db
        .prepare(
          `UPDATE feedback_jobs
           SET status = 'failed',
               anthropic_processing_status = 'ended',
               error_text = ?,
               updated_at = ?
           WHERE id = ?`,
        )
        .bind(asErrorMessage(error), now, resultLine.custom_id)
        .run();
    }

    return;
  }

  if (resultLine.result.type === "errored") {
    const message =
      resultLine.result.error?.error?.message ?? "Anthropic batch item failed";

    await db
      .prepare(
        `UPDATE feedback_jobs
         SET status = 'failed',
             anthropic_processing_status = 'ended',
             error_text = ?,
             updated_at = ?
         WHERE id = ?`,
      )
      .bind(message, now, resultLine.custom_id)
      .run();

    return;
  }

  if (resultLine.result.type === "canceled") {
    await db
      .prepare(
        `UPDATE feedback_jobs
         SET status = 'canceled',
             anthropic_processing_status = 'ended',
             updated_at = ?
         WHERE id = ?`,
      )
      .bind(now, resultLine.custom_id)
      .run();

    return;
  }

  await db
    .prepare(
      `UPDATE feedback_jobs
       SET status = 'expired',
           anthropic_processing_status = 'ended',
           updated_at = ?
       WHERE id = ?`,
    )
    .bind(now, resultLine.custom_id)
    .run();
}

async function getFeedbackRow(
  db: D1Database,
  id: string,
): Promise<FeedbackRow | null> {
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
         updated_at
       FROM feedback_jobs
       WHERE id = ?`,
    )
    .bind(id)
    .first<FeedbackRow>();

  return row ?? null;
}

function normalizeFeedbackRow(row: FeedbackRow) {
  return {
    ...row,
    result: row.result_json ? safeJsonParse(row.result_json) : null,
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
  const cleaned = text.replace(/^```json\s*/i, "").replace(/```$/i, "").trim();
  const parsed = JSON.parse(cleaned) as Partial<FeedbackAnalysis>;

  if (
    parsed &&
    typeof parsed.category === "string" &&
    typeof parsed.priority === "string" &&
    typeof parsed.summary === "string" &&
    FEEDBACK_CATEGORIES.includes(parsed.category as FeedbackCategory) &&
    FEEDBACK_PRIORITIES.includes(parsed.priority as FeedbackPriority)
  ) {
    return {
      category: parsed.category as FeedbackCategory,
      priority: parsed.priority as FeedbackPriority,
      summary: parsed.summary.trim(),
    };
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
    FEEDBACK_SOURCES.includes(value as FeedbackSource)
    ? (value as FeedbackSource)
    : null;
}

function asFeedbackJob(value: unknown): FeedbackJob {
  const job = value as Partial<FeedbackJob> | null;

  if (
    job &&
    typeof job === "object" &&
    typeof job.id === "string" &&
    typeof job.source === "string" &&
    typeof job.text === "string" &&
    typeof job.submittedAt === "string"
  ) {
    return job as FeedbackJob;
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
    </style>
  </head>
  <body>
    <main>
      <h1>Feedback Demo</h1>
      <p>Submit feedback, persist it in D1, queue it, and poll Anthropic batch results.</p>
      <form id="feedback-form">
        <select name="source" required>
          ${FEEDBACK_SOURCES.map(
            (source) => `<option value="${source}">${source}</option>`,
          ).join("")}
        </select>
        <textarea name="text" placeholder="What did the user say?" required></textarea>
        <button type="submit">Queue Feedback</button>
      </form>
      <p class="hint">This page polls the saved D1 record after submission. Queue delivery is configured for 3 messages or 60 seconds.</p>
      <pre id="output">Waiting for submission...</pre>
    </main>
    <script>
      const form = document.getElementById("feedback-form");
      const output = document.getElementById("output");
      let pollTimer = null;

      function stopPolling() {
        if (pollTimer) {
          clearInterval(pollTimer);
          pollTimer = null;
        }
      }

      async function pollFeedback(id) {
        const response = await fetch("/api/feedback/" + id);
        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);

        const status = body.feedback && body.feedback.status;
        if (${JSON.stringify(Array.from(TERMINAL_STATUSES))}.includes(status)) {
          stopPolling();
        }
      }

      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        stopPolling();

        const formData = new FormData(form);
        const payload = {
          source: formData.get("source"),
          text: formData.get("text"),
        };

        output.textContent = "Submitting...";

        const response = await fetch("/feedback", {
          method: "POST",
          headers: {
            "content-type": "application/json",
          },
          body: JSON.stringify(payload),
        });

        const body = await response.json();
        output.textContent = JSON.stringify(body, null, 2);

        if (body.ok && body.id) {
          await pollFeedback(body.id);
          pollTimer = setInterval(() => pollFeedback(body.id), 2000);
        }
      });
    </script>
  </body>
</html>`;
}
