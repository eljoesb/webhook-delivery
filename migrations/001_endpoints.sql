CREATE TABLE endpoints (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);