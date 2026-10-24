CREATE TABLE notasap_jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            SMALLINT    NOT NULL,
    status          SMALLINT    NOT NULL,
    payload         JSONB       NOT NULL,
    attempt         INT         NOT NULL DEFAULT 0,
    max_attempts    INT         NOT NULL,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deadline        TIMESTAMPTZ,
    lease           BIGINT      NOT NULL DEFAULT 0,
    idempotency_key TEXT,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT notasap_jobs_status_check CHECK (status BETWEEN 1 AND 5)
);
CREATE INDEX notasap_jobs_claim_idx   ON notasap_jobs (kind, run_at, id)   WHERE status = 1;
CREATE INDEX notasap_jobs_rescue_idx  ON notasap_jobs (deadline)           WHERE status = 2;
CREATE INDEX notasap_jobs_janitor_idx ON notasap_jobs (status, updated_at) WHERE status IN (3, 4, 5);
CREATE UNIQUE INDEX notasap_jobs_idempotency_idx ON notasap_jobs (kind, idempotency_key) WHERE idempotency_key IS NOT NULL;
