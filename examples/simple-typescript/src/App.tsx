import { useEffect, useMemo, useState } from 'react';
import {
  Auth,
  createClient,
  type RowData,
} from '@kalamdb/client';
import './styles.css';

type ActivityItem = {
  id: string;
  service: string;
  level: string;
  actor: string;
  message: string;
  createdAt: string;
};

const FEED_SQL = [
  'SELECT id, service, level, actor, message, created_at',
  'FROM demo.activity_feed',
].join(' ');

const client = createClient({
  url: import.meta.env.VITE_KALAMDB_URL ?? 'http://127.0.0.1:8080',
  authProvider: async () => Auth.basic(
    import.meta.env.VITE_KALAMDB_USER ?? 'demo-user',
    import.meta.env.VITE_KALAMDB_PASSWORD ?? 'demo123',
  ),
  disableCompression: true,
});

function readText(row: RowData, key: string): string {
  return row[key]?.asString() ?? '';
}

function toActivity(row: RowData): ActivityItem {
  return {
    id: readText(row, 'id'),
    service: readText(row, 'service'),
    level: readText(row, 'level'),
    actor: readText(row, 'actor'),
    message: readText(row, 'message'),
    createdAt: readText(row, 'created_at'),
  };
}

function readTimestamp(row: RowData, key: string): number {
  return row[key]?.asDate()?.getTime() ?? row[key]?.asInt() ?? 0;
}

function sortRecentActivity(rows: RowData[]): RowData[] {
  return [...rows]
    .sort((left, right) => readTimestamp(right, 'created_at') - readTimestamp(left, 'created_at'))
    .slice(0, 12);
}

export function App() {
  const [items, setItems] = useState<ActivityItem[]>([]);
  const [status, setStatus] = useState<'loading' | 'live' | 'error'>('loading');
  const [error, setError] = useState<string | null>(null);
  const [service, setService] = useState('api');
  const [level, setLevel] = useState('ok');
  const [actor, setActor] = useState('console');
  const [message, setMessage] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const summary = useMemo(() => {
    const critical = items.filter((item) => item.level === 'critical').length;
    const warnings = items.filter((item) => item.level === 'warn').length;
    const services = new Set(items.map((item) => item.service)).size;
    return { critical, warnings, services };
  }, [items]);

  useEffect(() => {
    let active = true;
    let unsubscribe: (() => Promise<void>) | undefined;

    const start = async (): Promise<void> => {
      try {
        unsubscribe = await client.live(
          FEED_SQL,
          (rows) => {
            if (!active) {
              return;
            }

            setItems(sortRecentActivity(rows).map(toActivity));
            setStatus('live');
          },
          {
            subscriptionOptions: { last_rows: 12 },
            onError: (event) => {
              if (!active) {
                return;
              }

              setStatus('error');
              setError(`Subscription dropped (${event.code}): ${event.message}`);
            },
          },
        );
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
      void unsubscribe?.();
      void client.disconnect();
    };
  }, []);

  const submitEvent = async (event: React.FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    const trimmed = message.trim();
    if (!trimmed) {
      return;
    }

    setSubmitting(true);
    setError(null);

    try {
      await client.query(
        'INSERT INTO demo.activity_feed (service, level, actor, message) VALUES ($1, $2, $3, $4)',
        [service, level, actor, trimmed],
      );
      setMessage('');
    } catch (caughtError) {
      setError(caughtError instanceof Error ? caughtError.message : String(caughtError));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <main className="shell">
      <section className="hero">
        <div>
          <p className="eyebrow">Browser SDK example</p>
          <h1>Realtime Ops Feed</h1>
          <p className="lede">
            One query to load the feed. One subscription to keep every tab current.
            No custom transport layer in between.
          </p>
        </div>
        <div className={`status status-${status}`} data-testid="connection-status">
          <span className="status-dot" />
          {status === 'live' ? 'Live' : status === 'loading' ? 'Connecting' : 'Needs attention'}
        </div>
      </section>

      <section className="summary-grid">
        <article>
          <span>Tracked rows</span>
          <strong>{items.length}</strong>
        </article>
        <article>
          <span>Critical</span>
          <strong>{summary.critical}</strong>
        </article>
        <article>
          <span>Warnings</span>
          <strong>{summary.warnings}</strong>
        </article>
        <article>
          <span>Services</span>
          <strong>{summary.services}</strong>
        </article>
      </section>

      <section className="panel composer-panel">
        <div className="panel-head">
          <h2>Insert an event</h2>
          <p>Submit once and watch every open tab update.</p>
        </div>
        <form className="composer" onSubmit={submitEvent}>
          <label>
            Service
            <input value={service} onChange={(event) => setService(event.target.value)} />
          </label>
          <label>
            Level
            <select value={level} onChange={(event) => setLevel(event.target.value)}>
              <option value="ok">ok</option>
              <option value="warn">warn</option>
              <option value="critical">critical</option>
            </select>
          </label>
          <label>
            Actor
            <input value={actor} onChange={(event) => setActor(event.target.value)} />
          </label>
          <label className="message-field">
            Message
            <textarea
              value={message}
              onChange={(event) => setMessage(event.target.value)}
              placeholder="Describe the thing worth broadcasting to every open dashboard"
            />
          </label>
          <button type="submit" disabled={submitting || status === 'error'}>
            {submitting ? 'Sending…' : 'Broadcast event'}
          </button>
        </form>
        {error ? <p className="error-text">{error}</p> : null}
      </section>

      <section className="panel">
        <div className="panel-head">
          <h2>Live feed</h2>
          <p>The SDK keeps the current row set materialized so the UI only renders snapshots.</p>
        </div>
        <div className="feed" data-testid="feed-list">
          {items.map((item) => (
            <article className="feed-row" data-testid="feed-row" key={item.id}>
              <header>
                <span className={`pill pill-${item.level}`}>{item.level}</span>
                <strong>{item.service}</strong>
                <span>{item.actor}</span>
              </header>
              <p>{item.message}</p>
              <small>{item.createdAt}</small>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
