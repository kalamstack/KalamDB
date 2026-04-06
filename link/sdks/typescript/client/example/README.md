# TypeScript SDK Example (Browser) - Build & Run

This example app lives in `link/sdks/typescript/client/example/` and uses the local SDK package from `link/sdks/typescript/client/`.

## Prerequisites

- Rust toolchain
- Node.js >= 18
- `wasm-pack` installed (`cargo install wasm-pack`)
- A running KalamDB server

## Step-by-step

### 1) Start the server

From the repo root:

```bash
cd backend
cargo run
```

Keep this running.

### 2) Build the TypeScript SDK (generates `dist/`)

In a new terminal, from the repo root:

```bash
cd link/sdks/typescript/client
npm install
npm run build
```

This compiles the Rust crate to WASM and produces output in `link/sdks/typescript/client/dist/`.

### 3) Install example dependencies

```bash
cd link/sdks/typescript/client/example
npm install
```

The example depends on the local SDK via `"@kalamdb/client": "file:.."`.

### 4) Run the example

```bash
npm run start
```

This starts a static server and opens the page.

- URL: `http://localhost:3000/example/index.html`

### 5) Use the UI

- Confirm **Server URL** is `http://localhost:8080`
- For local dev, use:
  - **Username**: `root`
  - **Password**: *(empty)*
- Click **Initialize WASM**
- Click **▶️ Run All Tests**

## Troubleshooting

### I changed the SDK but the example didn’t pick it up

Rebuild the SDK, then refresh the browser:

```bash
cd link/sdks/typescript/client
npm run build
```

### Port 3000 is in use

Run the server on a different port:

```bash
cd example
npx http-server .. -p 3001 -c-1
```

Then open `http://localhost:3001/example/index.html`.

### Authentication errors (401)

- Make sure the server is running.
- Make sure you’re using `root` with an empty password for local dev.
- Check server logs for auth failures.
