import { useEffect, useRef, useState } from 'react';
import { Auth, MessageType, createClient, type RowData } from 'kalam-link';
import './styles.css';

type ChatMessage = {
  id: string;
  role: string;
  author: string;
  content: string;
  createdAt: string;
};

type AgentEvent = {
  id: string;
  responseId: string;
  stage: string;
  preview: string;
  message: string;
  createdAt: string;
};

type LiveDraft = {
  stage: 'thinking' | 'typing' | 'saving';
  label: string;
  preview: string;
};

const ROOM = import.meta.env.VITE_CHAT_ROOM ?? 'main';
const ROOM_SQL = ROOM.replace(/'/g, "''");
const CHAT_USERNAME = import.meta.env.VITE_KALAMDB_USERNAME ?? 'admin';
const CHAT_SQL = [
  'SELECT id, role, author, content, created_at',
  'FROM chat_demo.messages',
  `WHERE room = '${ROOM_SQL}'`,
  'ORDER BY created_at ASC, id ASC',
].join(' ');
const EVENTS_SQL = [
  'SELECT id, response_id, stage, preview, message, created_at',
  'FROM chat_demo.agent_events',
  `WHERE room = '${ROOM_SQL}'`,
  'ORDER BY created_at ASC, id ASC',
].join(' ');

function createAuthedClient() {
  return createClient({
    url: import.meta.env.VITE_KALAMDB_URL ?? 'http://127.0.0.1:8080',
    authProvider: async () => Auth.basic(
      CHAT_USERNAME,
      import.meta.env.VITE_KALAMDB_PASSWORD ?? 'kalamdb123',
    ),
  });
}

const chatClient = createAuthedClient();
const eventsClient = createAuthedClient();

function text(row: RowData, key: string): string {
  return row[key]?.asString() ?? '';
}

function formatTimestamp(rawValue: string): string {
  const numericValue = Number(rawValue);
  if (!Number.isFinite(numericValue) || numericValue <= 0) {
    return rawValue;
  }

  const epochMs = numericValue > 1_000_000_000_000_000 ? Math.floor(numericValue / 1000) : numericValue;
  const timestamp = new Date(epochMs);
  if (Number.isNaN(timestamp.getTime())) {
    return rawValue;
  }

  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  }).format(timestamp);
}

function toMessage(row: RowData): ChatMessage {
  return {
    id: text(row, 'id'),
    role: text(row, 'role'),
    author: text(row, 'author'),
    content: text(row, 'content'),
    createdAt: formatTimestamp(text(row, 'created_at')),
  };
}

function toAgentEvent(row: RowData): AgentEvent {
  return {
    id: text(row, 'id'),
    responseId: text(row, 'response_id'),
    stage: text(row, 'stage'),
    preview: text(row, 'preview'),
    message: text(row, 'message'),
    createdAt: formatTimestamp(text(row, 'created_at')),
  };
}

function deriveLiveDraft(events: AgentEvent[]): LiveDraft | null {
  let activeEvent: AgentEvent | null = null;

  for (const event of events) {
    if (event.stage === 'thinking' || event.stage === 'typing' || event.stage === 'message_saved') {
      activeEvent = event;
    }

    if (event.stage === 'complete' && activeEvent?.responseId === event.responseId) {
      activeEvent = null;
    }
  }

  if (!activeEvent) {
    return null;
  }

  if (activeEvent.stage === 'thinking') {
    return {
      stage: 'thinking',
      label: 'KalamDB Copilot is thinking',
      preview: 'Planning the reply and preparing the first streamed tokens…',
    };
  }

  if (activeEvent.stage === 'message_saved') {
    return {
      stage: 'saving',
      label: 'KalamDB Copilot is committing the reply',
      preview: activeEvent.preview,
    };
  }

  return {
    stage: 'typing',
    label: 'KalamDB Copilot is streaming characters',
    preview: activeEvent.preview,
  };
}

export function App() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [events, setEvents] = useState<AgentEvent[]>([]);
  const [draft, setDraft] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [status, setStatus] = useState<'connecting' | 'live' | 'error'>('connecting');
  const [error, setError] = useState<string | null>(null);
  const threadRef = useRef<HTMLDivElement | null>(null);
  const refreshRef = useRef<() => Promise<void>>(async () => undefined);
  const liveDraft = deriveLiveDraft(events);

  useEffect(() => {
    let active = true;
    const unsubscribers: Array<() => Promise<void>> = [];
    let messagesRefreshInFlight: Promise<void> | null = null;
    let eventsRefreshInFlight: Promise<void> | null = null;

    const refreshMessages = async (): Promise<void> => {
      const rows = await chatClient.queryAll(CHAT_SQL);
      if (active) {
        setMessages(rows.map(toMessage).slice(-40));
      }
    };

    const refreshEvents = async (): Promise<void> => {
      const rows = await eventsClient.queryAll(EVENTS_SQL);
      if (active) {
        setEvents(rows.map(toAgentEvent).slice(-24));
      }
    };

    const queueMessagesRefresh = (): void => {
      if (messagesRefreshInFlight) {
        return;
      }

      messagesRefreshInFlight = refreshMessages()
        .catch((caughtError) => {
          if (!active) {
            return;
          }
          setStatus('error');
          setError(caughtError instanceof Error ? caughtError.message : String(caughtError));
        })
        .finally(() => {
          messagesRefreshInFlight = null;
        });
    };

    const queueEventsRefresh = (): void => {
      if (eventsRefreshInFlight) {
        return;
      }

      eventsRefreshInFlight = refreshEvents()
        .catch((caughtError) => {
          if (!active) {
            return;
          }
          setStatus('error');
          setError(caughtError instanceof Error ? caughtError.message : String(caughtError));
        })
        .finally(() => {
          eventsRefreshInFlight = null;
        });
    };

    refreshRef.current = refreshMessages;

    const start = async (): Promise<void> => {
      try {
        await refreshMessages();
        await refreshEvents();
        const messagesUnsubscribe = await chatClient.subscribeWithSql(CHAT_SQL, (event) => {
          if (event.type === MessageType.SubscriptionAck) {
            return;
          }
          if (event.type === MessageType.Error) {
            setStatus('error');
            setError('Message subscription failed. Check the agent output and live event feed.');
            return;
          }
          queueMessagesRefresh();
        }, { last_rows: 200 });
        unsubscribers.push(messagesUnsubscribe);
        const eventsUnsubscribe = await eventsClient.subscribeWithSql(EVENTS_SQL, (event) => {
          if (event.type === MessageType.SubscriptionAck) {
            return;
          }
          if (event.type === MessageType.Error) {
            setStatus('error');
            setError('Event stream subscription failed. Check the agent output and server state.');
            return;
          }
          queueEventsRefresh();
        }, { last_rows: 200 });
        unsubscribers.push(eventsUnsubscribe);
        if (active) {
          setStatus('live');
        }
      } catch (caughtError) {
        if (!active) {
          return;
        }
        setStatus('error');
        setError(caughtError instanceof Error ? caughtError.message : String(caughtError));
      }
    };

    void start();

    return () => {
      active = false;
      for (const unsubscribe of unsubscribers) {
        void unsubscribe();
      }
      void chatClient.disconnect();
      void eventsClient.disconnect();
    };
  }, []);

  useEffect(() => {
    const thread = threadRef.current;
    if (!thread) {
      return;
    }

    thread.scrollTop = thread.scrollHeight;
  }, [liveDraft?.preview, messages]);

  const send = async (event: React.FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    const content = draft.trim();
    if (!content) {
      return;
    }

    try {
      setIsSubmitting(true);
      setError(null);
      await chatClient.query(
        'INSERT INTO chat_demo.messages (room, role, author, sender_username, content) VALUES ($1, $2, $3, $4, $5)',
        [ROOM, 'user', CHAT_USERNAME, CHAT_USERNAME, content],
      );
      await refreshRef.current();
      setDraft('');
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : String(caughtError));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <main className="chat-shell">
      <section className="chat-hero">
        <div>
          <p className="eyebrow">USER table + STREAM table + EXECUTE AS USER</p>
          <h1>Chat With AI</h1>
          <p>
            The browser writes to a USER table, subscribes to a STREAM table, and watches the agent draft replies in real time before the final assistant row is committed.
          </p>
        </div>
        <div className={`status status-${status}`} data-testid="chat-status">
          <span className="status-dot" />
          {status === 'live' ? 'Live' : status === 'connecting' ? 'Connecting' : 'Stopped'}
        </div>
      </section>

      <section className="chat-panel">
        <div className="chat-layout">
          <div className="chat-main">
            <div className="chat-thread" data-testid="chat-thread" ref={threadRef}>
              {messages.map((message) => (
                <article className={`bubble bubble-${message.role}`} key={message.id}>
                  <header>
                    <strong>{message.author}</strong>
                    <span>{message.createdAt}</span>
                  </header>
                  <p>{message.content}</p>
                </article>
              ))}
              {liveDraft ? (
                <article className="bubble bubble-assistant bubble-live" data-testid="stream-preview">
                  <header>
                    <strong>KalamDB Copilot</strong>
                    <span>{liveDraft.label}</span>
                  </header>
                  <div className="bubble-live-status" data-testid="write-status">
                    <span className="typing-indicator" aria-hidden="true">
                      <span />
                      <span />
                      <span />
                    </span>
                    <span>
                      {liveDraft.stage === 'thinking'
                        ? 'Thinking through the reply'
                        : liveDraft.stage === 'saving'
                          ? 'Saving the final assistant message'
                          : 'Writing the reply'}
                    </span>
                  </div>
                  <p>{liveDraft.preview}</p>
                </article>
              ) : null}
            </div>

            <form className="composer" onSubmit={send}>
              <label>
                Message
                <textarea
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  placeholder="Ask about latency, deploys, queues, or anything else you want the worker to stream back"
                />
              </label>
              <div className="composer-actions">
                <button type="submit" disabled={isSubmitting}>
                  {isSubmitting ? 'Sending…' : 'Send through KalamDB'}
                </button>
                {liveDraft ? (
                  <span className="composer-writing-hint">
                    Live reply in progress
                  </span>
                ) : null}
              </div>
            </form>
            {error ? <p className="error-text">{error}</p> : null}
          </div>

          <aside className="event-rail">
            <header className="event-rail-header">
              <strong>Live agent events</strong>
              <span>{events.length} buffered</span>
            </header>
            <ul className="event-list" data-testid="agent-events">
              {events.slice(-8).reverse().map((event) => (
                <li className="event-item" key={`${event.id}-${event.responseId}`}>
                  <div>
                    <strong>{event.stage}</strong>
                    <p>{event.message}</p>
                  </div>
                  <span>{event.createdAt}</span>
                </li>
              ))}
            </ul>
          </aside>
        </div>
      </section>
    </main>
  );
}