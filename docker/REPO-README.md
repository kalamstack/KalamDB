# 🚀 KalamDB on Docker Hub

KalamDB is a SQL-first realtime state database for AI agents, chat products, and multi-tenant SaaS.

It combines in one runtime:

- ⚡ SQL reads and writes
- 🔄 Live subscriptions
- 📡 Pub/sub streams
- 🔐 Built-in auth with JWT support
- 🧱 Multi-tenant isolation
- 💾 Hot storage on RocksDB
- 📦 Cold storage on Parquet
- 🛠️ Included CLI tools in the image

The Docker image includes:

- `kalamdb-server`
- `kalam`
- `kalam-cli`

## ✨ Quick Start

### 🐳 Pull and run

```bash
docker pull jamals86/kalamdb:latest

docker run -d \
  --name kalamdb \
  -p 8080:8080 \
  -e KALAMDB_SERVER_HOST=0.0.0.0 \
  -e KALAMDB_ROOT_PASSWORD=kalamdb123 \
  -e KALAMDB_JWT_SECRET=replace-with-a-32-char-secret \
  -v kalamdb_data:/data \
  jamals86/kalamdb:latest
```

### ✅ Check health

```bash
curl http://localhost:8080/health
curl http://localhost:8080/v1/api/healthcheck
```

### 🔑 Login

```bash
curl -X POST http://localhost:8080/v1/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"root","password":"kalamdb123"}'
```

### 🧪 Run your first SQL query

```bash
curl -X POST http://localhost:8080/v1/api/sql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT 1 AS ok"}'
```

## 🧩 Docker Compose

### 📄 Single-node compose file

- GitHub view: https://github.com/jamals86/KalamDB/blob/main/docker/run/single/docker-compose.yml
- Raw file: https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/single/docker-compose.yml

### 🧱 3-node cluster compose file

- GitHub view: https://github.com/jamals86/KalamDB/blob/main/docker/run/cluster/docker-compose.yml
- Raw file: https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/cluster/docker-compose.yml

### ⚡ Start single node with compose

```bash
cd docker/run/single
export KALAMDB_JWT_SECRET=replace-with-a-32-char-secret
docker compose up -d
```

By default, the single-node compose file publishes KalamDB on `http://localhost:8088`.

### ⚡ Start 3-node cluster with compose

```bash
cd docker/run/cluster
export KALAMDB_JWT_SECRET=replace-with-a-32-char-secret
docker compose up -d
```

Cluster endpoints:

- Node 1: `http://localhost:8081`
- Node 2: `http://localhost:8082`
- Node 3: `http://localhost:8083`

### 🌐 One-line startup from the repo

Single node:

```bash
KALAMDB_JWT_SECRET=replace-with-a-32-char-secret \
curl -sSL https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/single/docker-compose.yml | docker compose -f - up -d
```

3-node cluster:

```bash
KALAMDB_JWT_SECRET=replace-with-a-32-char-secret \
curl -sSL https://raw.githubusercontent.com/jamals86/KalamDB/main/docker/run/cluster/docker-compose.yml | docker compose -f - up -d
```

## 🖥️ Open the UI

If the Admin UI is enabled in your image and config, open:

- Single container run: `http://localhost:8080`
- Single-node compose: `http://localhost:8088`
- Cluster node 1: `http://localhost:8081`

Useful API endpoints:

- Health: `GET /health`
- Extended health: `GET /v1/api/healthcheck`
- Login: `POST /v1/api/auth/login`
- SQL API: `POST /v1/api/sql`

## 🛠️ CLI Commands

The image includes both `kalam` and `kalam-cli`.

### 📌 Basic CLI usage

```bash
docker exec -it kalamdb kalam --version
docker exec -it kalamdb kalam-cli --version
```

### 🐚 Open a shell inside the container

The image includes bash for direct shell access:

```bash
docker exec -it kalamdb bash
```

### ▶️ Useful commands to run inside the container

```bash
docker exec -it kalamdb kalam --help
docker exec -it kalamdb kalam-cli --help
docker exec -it kalamdb /usr/local/bin/kalamdb-server --help
```

### 📋 Common Docker commands

```bash
# View logs
docker logs -f kalamdb

# Stop container
docker stop kalamdb

# Start container
docker start kalamdb

# Remove container
docker rm -f kalamdb

# List volumes
docker volume ls

# Inspect persisted data volume
docker volume inspect kalamdb_data
```

## 🌱 Environment Variables

These are the most useful environment variables for setup and day-to-day use.

| Variable | Purpose | Example |
| --- | --- | --- |
| `KALAMDB_SERVER_HOST` | Bind address inside the container | `0.0.0.0` |
| `KALAMDB_ROOT_PASSWORD` | Root user password for initial login | `kalamdb123` |
| `KALAMDB_JWT_SECRET` | JWT signing secret, should be at least 32 chars | `super-secret-value` |
| `KALAMDB_LOG_LEVEL` | Logging level | `info` |
| `KALAMDB_PORT` | Host port override in single-node compose | `8080` |
| `KALAMDB_ALLOW_REMOTE_SETUP` | Allow initial remote setup in Docker-based development flows | `true` |
| `KALAMDB_CLUSTER_ID` | Cluster identifier for multi-node deployments | `docker-cluster` |
| `KALAMDB_NODE_ID` | Node id in cluster setups | `1` |
| `KALAMDB_CLUSTER_RPC_ADDR` | Raft RPC address for a node | `kalamdb-node1:9090` |
| `KALAMDB_CLUSTER_API_ADDR` | Public API address for a node | `http://kalamdb-node1:8080` |
| `KALAMDB_CLUSTER_PEERS` | Cluster peer list for Docker cluster compose | `2@node2:9090@http://node2:8080;3@node3:9090@http://node3:8080` |

### 🔐 Security note

For anything beyond local testing:

- change `KALAMDB_ROOT_PASSWORD`
- use a strong `KALAMDB_JWT_SECRET`
- persist `/data` with a Docker volume or bind mount

## 💾 Persistence

KalamDB stores database state under `/data` inside the container.

Run with a named volume:

```bash
docker run -d \
  --name kalamdb \
  -p 8080:8080 \
  -e KALAMDB_SERVER_HOST=0.0.0.0 \
  -e KALAMDB_ROOT_PASSWORD=kalamdb123 \
  -e KALAMDB_JWT_SECRET=replace-with-a-32-char-secret \
  -v kalamdb_data:/data \
  jamals86/kalamdb:latest
```

Backup example:

```bash
docker run --rm \
  -v kalamdb_data:/data \
  -v "$PWD":/backup \
  alpine \
  tar czf /backup/kalamdb-data.tar.gz /data
```

## 🔍 Troubleshooting

### Container is up but you cannot connect

```bash
docker logs kalamdb
```

Make sure:

- port `8080` is published
- `KALAMDB_SERVER_HOST=0.0.0.0`
- your JWT secret is set when binding beyond localhost

### Login is failing

Make sure the password you use matches `KALAMDB_ROOT_PASSWORD` from container startup.

### You see `Permission denied (os error 13)` on startup

The image runs as a non-root user.

The bundled CLI uses `/data/.kalam` as its default local state directory inside the container.

Check these first:

- if you mounted a custom `server.toml`, make sure the file is readable inside the container
- if your custom config changes `logs_path` or `data_path`, make sure those paths are writable
- if you bind-mount a host directory to `/data` or `/config`, make sure Docker grants the container write access
- for the simplest persistent setup, prefer a named Docker volume for `/data`

### You want a clean reset

```bash
docker rm -f kalamdb
docker volume rm kalamdb_data
```

## 🔗 Links

- Docker Hub: https://hub.docker.com/r/jamals86/kalamdb
- GitHub: https://github.com/jamals86/KalamDB
- Docs: https://kalamdb.org/docs
- Single-node compose: https://github.com/jamals86/KalamDB/blob/main/docker/run/single/docker-compose.yml
- Cluster compose: https://github.com/jamals86/KalamDB/blob/main/docker/run/cluster/docker-compose.yml