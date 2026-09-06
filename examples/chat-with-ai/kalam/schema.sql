-- Chat With AI
--
-- A multi-user room chat on KalamDB. There is no polling and no extra backend
-- besides a small topic worker in src/agent.ts.
--
-- How a sent message travels:
--   1. The browser inserts a row into chat_demo.messages.
--   2. Topic chat_demo.ai_inbox receives that INSERT.
--   3. src/agent.ts consumes the topic and writes STREAM rows while drafting.
--   4. The agent inserts the assistant reply as DBA (SHARED tables reject
--      EXECUTE AS USER). STREAM thinking/typing rows still use AS USER.
--   5. Every member tab sees both steps through live queries.
--
-- Table kinds used here:
--   SHARED  one copy of the data, then CREATE POLICY decides who sees which rows
--   STREAM  ephemeral progress rows (thinking / typing) with a short TTL
--   TOPIC   fan-out of INSERTs so a worker can react without polling

CREATE NAMESPACE IF NOT EXISTS chat_demo;

-- This file is the source of truth for `kalam dev`. Dropping first lets the
-- same script recreate a clean local demo.
DROP TABLE IF EXISTS chat_demo.agent_events;
DROP TABLE IF EXISTS chat_demo.messages;
DROP TABLE IF EXISTS chat_demo.room_members;
DROP TABLE IF EXISTS chat_demo.rooms;

-- Rooms everyone can create. SELECT is limited to rooms the user belongs to.
CREATE SHARED TABLE IF NOT EXISTS chat_demo.rooms (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Membership. One TEXT primary key: KalamDB does not support composite PKs yet.
-- id is '{user_id}:{room_id}' so a user can join a room at most once.
CREATE SHARED TABLE IF NOT EXISTS chat_demo.room_members (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    room_id TEXT NOT NULL
);

-- Durable chat transcript. Policies keep each user inside rooms they joined.
CREATE SHARED TABLE IF NOT EXISTS chat_demo.messages (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    room TEXT NOT NULL DEFAULT 'main',
    role TEXT NOT NULL,
    author TEXT NOT NULL,
    sender_username TEXT NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_room ON chat_demo.messages (room);
CREATE INDEX IF NOT EXISTS idx_room_members_user ON chat_demo.room_members (user_id);

-- Live "the agent is thinking / typing" rows. STREAM + TTL so they fade away.
CREATE STREAM TABLE IF NOT EXISTS chat_demo.agent_events (
    id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
    response_id TEXT NOT NULL,
    room TEXT NOT NULL DEFAULT 'main',
    sender_username TEXT NOT NULL,
    stage TEXT NOT NULL,
    preview TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
) WITH (TTL_SECONDS = 10);

-- You only see rooms you have joined.
CREATE POLICY rooms_member_select ON chat_demo.rooms
  FOR SELECT TO user
  USING (
    id IN (
      SELECT room_id FROM chat_demo.room_members
      WHERE user_id = CURRENT_USER
    )
  );

-- Anyone signed in can create a room. Joining is a separate membership insert.
CREATE POLICY rooms_create ON chat_demo.rooms
  FOR INSERT TO user
  WITH CHECK (true);

-- You can add or remove only your own membership rows.
CREATE POLICY room_members_self ON chat_demo.room_members
  FOR ALL TO user
  USING (user_id = CURRENT_USER)
  WITH CHECK (user_id = CURRENT_USER);

-- Messages in a room are visible only to members of that room.
CREATE POLICY messages_member_select ON chat_demo.messages
  FOR SELECT TO user
  USING (
    room IN (
      SELECT room_id FROM chat_demo.room_members
      WHERE user_id = CURRENT_USER
    )
  );

-- You can only post into a room you belong to.
CREATE POLICY messages_member_insert ON chat_demo.messages
  FOR INSERT TO user
  WITH CHECK (
    room IN (
      SELECT room_id FROM chat_demo.room_members
      WHERE user_id = CURRENT_USER
    )
  );

-- Same membership rule for edits.
CREATE POLICY messages_member_update ON chat_demo.messages
  FOR UPDATE TO user
  USING (
    room IN (
      SELECT room_id FROM chat_demo.room_members
      WHERE user_id = CURRENT_USER
    )
  )
  WITH CHECK (
    room IN (
      SELECT room_id FROM chat_demo.room_members
      WHERE user_id = CURRENT_USER
    )
  );

-- Wake the agent on every new chat row. The worker ignores non-user roles.
CREATE TOPIC IF NOT EXISTS chat_demo.ai_inbox;
ALTER TOPIC chat_demo.ai_inbox ADD SOURCE chat_demo.messages ON INSERT;

-- Seed a default room so the first browser tab has somewhere to join.
INSERT INTO chat_demo.rooms (id, title)
VALUES ('main', 'Main');

INSERT INTO chat_demo.room_members (id, user_id, room_id)
VALUES ('root:main', 'root', 'main'), ('admin:main', 'admin', 'main');

INSERT INTO chat_demo.messages (role, author, sender_username, content)
VALUES ('user', 'user_1', 'root', 'Hello everyone!');

INSERT INTO chat_demo.messages (role, author, sender_username, content)
VALUES ('assistant', 'ai_bot', 'assistant', 'Hi, how can I help?');
