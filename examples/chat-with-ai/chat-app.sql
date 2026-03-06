CREATE NAMESPACE IF NOT EXISTS chat_demo;

DROP TABLE IF EXISTS chat_demo.agent_events;
DROP TABLE IF EXISTS chat_demo.messages;

CREATE TABLE IF NOT EXISTS chat_demo.messages (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    room TEXT NOT NULL DEFAULT 'main',
    role TEXT NOT NULL,
    author TEXT NOT NULL,
    sender_username TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) WITH (TYPE = 'USER');

CREATE TABLE IF NOT EXISTS chat_demo.agent_events (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    response_id TEXT NOT NULL,
    room TEXT NOT NULL DEFAULT 'main',
    sender_username TEXT NOT NULL,
    stage TEXT NOT NULL,
    preview TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) WITH (TYPE = 'STREAM', TTL_SECONDS = 120);
