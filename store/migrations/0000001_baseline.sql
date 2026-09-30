-- +goose Up

CREATE TABLE public.workflow_runs (
    id              UUID        NOT NULL,
    name            TEXT        NOT NULL,
    idempotency_key TEXT,
    input           JSONB       NOT NULL,
    status          SMALLINT    NOT NULL DEFAULT 1,
    current_task    SMALLINT    NOT NULL DEFAULT 0,
    claimed_at      TIMESTAMPTZ,
    claim_id        UUID,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at    TIMESTAMPTZ,
    call_context    JSONB,
    CONSTRAINT workflow_runs_pkey PRIMARY KEY (id),
    CONSTRAINT workflow_runs_status_check CHECK (status IN (1, 2, 3, 4)),
    CONSTRAINT workflow_runs_name_idempotency_key_key UNIQUE (name, idempotency_key)
);

CREATE INDEX workflow_runs_claimable_idx
    ON public.workflow_runs (next_retry_at)
    WHERE status = 1 AND claimed_at IS NULL;

CREATE TABLE public.workflow_tasks (
    run_id          UUID        NOT NULL REFERENCES public.workflow_runs(id) ON DELETE CASCADE,
    seq             SMALLINT    NOT NULL,
    handler         TEXT        NOT NULL,
    output          JSONB,
    status          SMALLINT    NOT NULL DEFAULT 1,
    attempt_count   INT         NOT NULL DEFAULT 0,
    last_error      TEXT,
    attempts        JSONB       NOT NULL DEFAULT '[]',
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error_code TEXT,
    CONSTRAINT workflow_tasks_pkey PRIMARY KEY (run_id, seq),
    CONSTRAINT workflow_tasks_status_check CHECK (status IN (1, 2, 3, 4, 5))
);

-- +goose Down

DROP TABLE IF EXISTS public.workflow_tasks;
DROP TABLE IF EXISTS public.workflow_runs;
