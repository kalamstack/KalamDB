# Realtime Ops Feed

Smallest useful browser example for the TypeScript SDK.

This app shows three KalamDB strengths with very little code:

- SQL reads with `queryAll()`
- live updates with `subscribeWithSql()`
- browser auth using `Auth.basic()` via the SDK's built-in JWT exchange

Instead of a CRUD todo app, this example renders a live operations feed. Open it in two tabs, add an event in one tab, and the second tab updates immediately.

## Why this example exists

It stays intentionally small so the SDK usage is obvious:

- one React component
- no client wrapper layer
- no custom hook hierarchy
- one browser test that verifies the real cross-tab flow

## Quick start

1. Make sure KalamDB is running on `http://127.0.0.1:8080`.
2. Run `npm install`.
3. Run `npm run setup`.
4. Run `npm run dev`.
5. Open `http://127.0.0.1:5173`.

The setup script creates:

- namespace `demo`
- user table `demo.activity_feed`
- local example user `demo-user` / `demo123`
- `.env.local` with browser credentials for local development

## Test it

Run `npm test`.

The Playwright test:

- boots the example
- opens two pages
- submits a new event
- verifies both tabs render the same live row

## Files worth reading

- `src/App.tsx`: complete example logic
- `activity-feed.sql`: schema used by the app
- `tests/realtime.spec.ts`: end-to-end verification
- [ ] **Offline sync**: Disconnect, add TODO elsewhere, reconnect, verify sync

### Automated Tests

```bash
npm test
```

(Test files to be added in future iteration)

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VITE_KALAMDB_URL` | Yes | - | WebSocket URL (e.g., `ws://localhost:8080`) |
| `VITE_KALAMDB_API_KEY` | Yes | - | API key from `kalam user create` |

### localStorage Keys

- `kalamdb_todos_cache`: Complete TODO list with metadata
- `kalamdb_last_sync_id`: Highest ID seen (for subscription resume)

Cache expires after 7 days of inactivity.

## Customization

### Change TODO Table Schema

Edit `todo-app.sql`:

```sql
CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    priority TEXT,  -- Add custom field
    tags TEXT       -- Add custom field
);
```

Re-run setup:

```bash
./setup.sh
```

Update types in `src/types/todo.ts`:

```typescript
export interface Todo {
  id: number;
  title: string;
  completed: boolean;
  created_at: string;
  priority?: string;  // Add custom field
  tags?: string;      // Add custom field
}
```

### Customize Styling

All styles are in `src/styles/App.css` using CSS variables:

```css
:root {
  --color-primary: #3b82f6;        /* Change primary color */
  --color-success: #10b981;        /* Change success color */
  --spacing-md: 1rem;              /* Adjust spacing */
  --radius-md: 0.5rem;             /* Change border radius */
}
```

## Troubleshooting

### "Cannot connect to KalamDB"

- ✅ Check server is running: `curl http://localhost:8080/health`
- ✅ Verify `VITE_KALAMDB_URL` matches server address
- ✅ Check firewall isn't blocking WebSocket connections

### "API key is required"

- ✅ Verify `.env` file exists (not just `.env.example`)
- ✅ Check `VITE_KALAMDB_API_KEY` is set correctly
- ✅ Restart dev server after changing `.env`

### TODOs not syncing

- ✅ Check connection status (should be green "Connected")
- ✅ Open browser console for errors
- ✅ Verify WebSocket connection in Network tab

### localStorage not working

- ✅ Check browser privacy settings (localStorage might be disabled)
- ✅ Clear cache: `localStorage.clear()` in console
- ✅ Check storage quota: Chrome DevTools → Application → Storage

## Performance

### Bundle Size

- **Uncompressed**: ~200 KB (React + app code)
- **Gzip**: ~60 KB
- **WASM module**: ~40 KB (when using real WASM client)

### Load Times

- **Initial load** (no cache): ~500ms
- **Initial load** (with cache): <50ms (instant render)
- **WebSocket connection**: ~100ms
- **Subscription setup**: ~50ms
- **Event propagation**: <100ms (server to all clients)

### Browser Support

- Chrome/Edge 90+
- Firefox 88+
- Safari 14+
- Mobile browsers (iOS Safari, Chrome Mobile)

## Production Deployment

### 1. Build for Production

```bash
npm run build
```

Output in `dist/` directory.

### 2. Use Real WASM Client

Replace mock client in `src/services/kalamClient.ts`:

```typescript
// TODO: Uncomment when WASM module is available
import init, { KalamClient } from '../../../link/sdks/typescript/pkg/kalam_link.js';

export async function createKalamClient(config: KalamClientConfig): Promise<IKalamClient> {
  await init(); // Initialize WASM
  return new KalamClient(config.url, config.apiKey);
}
```

### 3. Configure for Production

- Use `wss://` (WebSocket Secure) instead of `ws://`
- Enable HTTPS for web server
- Use environment-specific API keys
- Configure CORS on KalamDB server
- Set up monitoring and error tracking

### 4. Deploy

Serve `dist/` with any static file server:

```bash
# Using Python
python3 -m http.server 8000 --directory dist

# Using Caddy
caddy file-server --root dist --listen :8000

# Using Vercel
vercel deploy dist
```

## Learn More

- [KalamDB Documentation](../../docs/README.md)
- [WASM Client API](../../specs/006-docker-wasm-examples/contracts/wasm-client.md)
- [Quick Start Guide](../../specs/006-docker-wasm-examples/quickstart.md)
- [Architecture Overview](../../docs/architecture/README.md)

## License

Same as KalamDB main project.

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.
