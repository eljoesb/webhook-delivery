CREATE TABLE events (
    id UUID PRIMARY KEY,
    endpoint_id UUID NOT NULL REFERENCES endpoints (id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'in_progress', 'delivered', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX events_due_idx ON events (next_attempt_at)
    WHERE status IN ('pending', 'in_progress');

CREATE TABLE delivery_attempts (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL REFERENCES events (id),
    attempt_number INTEGER NOT NULL,
    http_status INTEGER,
    response_body TEXT,
    error_message TEXT,
    succeeded BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);