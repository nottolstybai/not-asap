-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS notasap_jobs (
    id              UUID        NOT NULL DEFAULT gen_random_uuid(),
    job_type        TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'new',
    run_after       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt         INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL DEFAULT 3,
    error           TEXT,
    last_heartbeat_at TIMESTAMPTZ,
    worker_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT notasap_jobs_pkey PRIMARY KEY (id),
    CONSTRAINT notasap_jobs_status_check CHECK (
        status IN ('new','running','done','failed','retrying','exhausted','cancelled')
    )
);
-- +goose StatementEnd

-- +goose StatementBegin
-- Index for polling: SELECT ... WHERE status IN ('new','retrying') AND run_after <= NOW() FOR UPDATE SKIP LOCKED
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_poll
    ON notasap_jobs (status, run_after)
    WHERE status IN ('new', 'retrying');
-- +goose StatementEnd

-- +goose StatementBegin
-- Index for stuck-job detection: SELECT ... WHERE status = 'running' AND last_heartbeat_at < NOW() - interval '60s'
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_heartbeat
    ON notasap_jobs (status, last_heartbeat_at)
    WHERE status = 'running';
-- +goose StatementEnd

-- +goose StatementBegin
-- Index for janitor: DELETE ... WHERE status IN ('done','failed','exhausted','cancelled') AND updated_at < ...
CREATE INDEX IF NOT EXISTS idx_notasap_jobs_janitor
    ON notasap_jobs (status, updated_at)
    WHERE status IN ('done', 'failed', 'exhausted', 'cancelled');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS notasap_jobs;
-- +goose StatementEnd
