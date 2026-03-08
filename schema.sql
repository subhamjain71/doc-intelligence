CREATE EXTENSION IF NOT EXISTS vector;

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

CREATE TABLE IF NOT EXISTS USERS (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    createdAt TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE FILES (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES USERS (id),
    filename TEXT NOT NULL,
    s3_key TEXT NOT NULL,
    size_bytes INTEGER,
    status TEXT NOT NULL DEFAULT 'pending_upload',
    -- pending-upload -> uploaded-> processing-> processed -> failed
    processing_started_at TIMESTAMPTZ,
    is_deleted BOOLEAN NOT NULL DEFAULT false,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    presigned_expiry TIMESTAMPTZ
);

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    payload JSON NOT NULL,
    published_at TIMESTAMPTZ,
    attempts INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE embeddings (
    id SERIAL PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files (id),
    user_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    page_number INTEGER,
    embedding vector (768),
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE conversations (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    create_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER NOT NULL REFERENCES conversations (id),
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    token_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);