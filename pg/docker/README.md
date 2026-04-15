# pg-kalam on Docker Hub

PostgreSQL with the pg_kalam extension and pgvector preinstalled so you can connect PostgreSQL Kalam-backed tables to a remote KalamDB server.

This image packages stock PostgreSQL with the `pg_kalam` foreign data wrapper extension and the `pgvector` package already installed.

## What you get

- A PostgreSQL runtime image with `pg_kalam.so`, `pg_kalam.control`, and the extension SQL files already installed
- `pgvector` preinstalled so `CREATE EXTENSION vector;` works without extra package installation
- `CREATE EXTENSION pg_kalam;` support without a separate extension build step in the container
- A ready-to-run base for local testing, CI, and production images that need PostgreSQL to talk to KalamDB

## Quick start

Pull the image:

```bash
docker pull jamals86/pg-kalam:latest
```

Run PostgreSQL with the extension available:

```bash
docker run --name pg-kalam \
  -e POSTGRES_DB=kalamdb \
  -e POSTGRES_USER=kalamdb \
  -e POSTGRES_PASSWORD=kalamdb123 \
  -p 5432:5432 \
  -d jamals86/pg-kalam:latest
```

Then enable the extension:

```sql
CREATE EXTENSION IF NOT EXISTS pg_kalam;
CREATE EXTENSION IF NOT EXISTS vector;
```

## Connect to KalamDB

After the extension is enabled, create a foreign server that points at your running KalamDB instance.

For a full local stack with PostgreSQL and KalamDB together, use the compose setup documented in [pg/README.md](../README.md).