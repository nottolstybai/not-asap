package database

// SQL constant for job insertion.
const InsertJobSQL = `
    INSERT INTO notasap_jobs (job_type, payload, status, max_attempts)
    VALUES ($1, $2, 'new', $3)
    RETURNING id, job_type, status, max_attempts, created_at`

// SQL for claiming a job atomically.
const ClaimJobSQL = `
    UPDATE notasap_jobs
    SET status = 'running', worker_id = $1, last_heartbeat_at = NOW()
    WHERE id = (
        SELECT id FROM notasap_jobs
        WHERE status IN ('new', 'retrying') AND run_after <= NOW()
        ORDER BY id
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING id, job_type, payload, attempt, max_attempts`

// SQL for marking a job as done.
const CompleteJobSQL = `
    UPDATE notasap_jobs
    SET status = $1, updated_at = NOW()
    WHERE id = $2 AND worker_id = $3 AND status = 'running'`

// SQL for retrying a job.
const RetryJobSQL = `
    UPDATE notasap_jobs
    SET status = $1, run_after = $2, attempt = $3, error = $4, updated_at = NOW()
    WHERE id = $5 AND worker_id = $6 AND status = 'running'`

// SQL for batch heartbeat.
const HeartbeatSQL = `
    UPDATE notasap_jobs
    SET last_heartbeat_at = NOW()
    WHERE id = ANY($1)`

// SQL for janitor cleanup. $2 is the cutoff timestamp (updated_at before this is eligible).
const JanitorDeleteSQL = `
    DELETE FROM notasap_jobs
    WHERE id IN (
        SELECT id FROM notasap_jobs
        WHERE status = $1 AND updated_at < $2
        LIMIT 1000
    )`

// Max error message length stored in the database.
const MaxErrorMsgLength = 4096
