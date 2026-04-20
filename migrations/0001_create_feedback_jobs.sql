CREATE TABLE IF NOT EXISTS feedback_jobs (
  id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  text TEXT NOT NULL,
  submitted_at TEXT NOT NULL,
  status TEXT NOT NULL,
  anthropic_batch_id TEXT,
  anthropic_processing_status TEXT,
  category TEXT,
  priority TEXT,
  summary TEXT,
  result_json TEXT,
  error_text TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_feedback_jobs_batch_status
  ON feedback_jobs (anthropic_processing_status, anthropic_batch_id);

CREATE INDEX IF NOT EXISTS idx_feedback_jobs_updated_at
  ON feedback_jobs (updated_at);
