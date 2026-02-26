# ASX Data

Database utilities and web visualiser for ASX stock market data.

## Project Structure
- `stockdb/` — data ingestion pipeline and SQLite3 database
- `www/` — Flask web application

## Running

### Locally
```bash
cd www && ./asx
```

### Docker
```bash
docker compose up
```

## Rules
- App must run both inside Docker (via compose) and directly via `./asx`
- DB is never copied into the Docker image — always mounted as a read-only volume
- DATABASE path is configured via env var; defaults to `../stockdb/stockdb.db` for local use
