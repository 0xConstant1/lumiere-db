# LumiereDB
![Go](https://img.shields.io/badge/Go-API-00ADD8?logo=go&logoColor=white) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white) ![ParadeDB](https://img.shields.io/badge/ParadeDB-pg_search-111111)

LumiereDB is an opinionated, self-hostable API built on IMDb's non-commercial datasets. The core philosophy is to do all the heavy pre-processing during the ETL phase so the API can be rapid without expensive runtime joins. The goal is a fast, pragmatic API for titles, search, and discovery. This project is for non-commercial, individual use only; see [IMDb Data Terms and Attribution](#imdb-data-terms-and-attribution). Think of it as an IMDb in your pocket for your homelab.

**Architecture**
- Go API service using Echo
- PostgreSQL with ParadeDB (pg_search/BM25)
- Batch ETL that downloads IMDb TSVs, loads staging tables, and materializes final tables

**Self-Host With Docker**

1. Start the stack:

```bash
docker compose up -d
```

2. The API container can run ETL in scheduler mode (default), serving requests while periodic ETL checks/rebuilds run in the background.

Default ports:
- API: `http://localhost:8000`
- Postgres: `localhost:5432`

**API Routes**

Base URL: `http://localhost:8000`

Titles:
- `GET /titles/{tconst}`  
Returns the full JSON blob for a title.

Search (BM25 via ParadeDB):
- `GET /search?query=...&type=series&limit=20`
- `GET /search?query=...&type=movies&limit=20`

Discover:
- `GET /discover?type=series&year_from=2020&year_to=2023&genres=horror,mystery&sort=popular&limit=20`
- `GET /discover?type=movies&genres=action,thriller&min_votes=10000&min_rating=7.0&sort=top_rated&limit=20`
- `GET /discover?type=movies&sort=newest&limit=20&cursor=...`

Discover query notes:
- `type` is required: `series` or `movies`
- `genres` (or `genre`) supports up to 3 values; when multiple are passed, all must match
- `sort`: `popular` (default), `top_rated`, `newest`, `oldest`
- `cursor` enables keyset pagination; response includes `meta.nextCursor`

**Environment Variables**

Core:
- `DATABASE_URL` (required)
- `PORT` (default: `8000`)
- `RUN_ETL` (default: `true`)
- `ENABLE_PG_SEARCH` (default: `true`)

IMDb datasets:
- `IMDB_BASE_URL` (default: `https://datasets.imdbws.com`)
- `IMDB_DATA_DIR` (default: `/data`)
- `IMDB_FORCE_DOWNLOAD` (default: `false`)
- `IMDB_FORCE_REBUILD` (default: `false`)
- `DATASET_DATE` (default: current UTC date)

Scheduler:
- `ETL_SCHEDULE_ENABLED` (default: `true`)
- `ETL_POLL_INTERVAL` (default: `1h`)
- `ETL_BOOTSTRAP_BLOCKING` (default: `true`)
- `ETL_SWAP_LOCK_TIMEOUT` (default: `30s`)

Update trigger rule:
- Scheduler polls IMDb with `HEAD` and compares `Last-Modified` + `Content-Length` per file.
- A full rebuild runs only when all tracked files changed versus the last successful run.

ETL controls:
- `ETL_SQL_DIR` (default: `etl` or `../etl`)
- `ETL_LOAD_BATCH_SIZE` (default: `10000`)
- `ETL_BATCH_SIZE` (default: `10000`)
- `ETL_DOWNLOAD_CONCURRENCY` (default: `3`)
- `ETL_KEEP_STAGING` (default: `false`)
- `ETL_MIN_NUMVOTES` (default: `1`)
- `ETL_MAX_ACTORS` (default: `10`)
- `ETL_MAX_PRODUCERS` (default: `1`)
- `ETL_MAX_WRITERS` (default: `1`)
- `ETL_MAX_DIRECTORS` (default: `1`)

Postgres tuning (optional):
- `ETL_MAX_PARALLEL_WORKERS`
- `ETL_WORK_MEM`
- `ETL_MAINTENANCE_WORK_MEM`
- `ETL_DB_MAX_WAL_SIZE`
- `ETL_DB_MIN_WAL_SIZE`
- `ETL_DB_CHECKPOINT_TIMEOUT`
- `ETL_DB_CHECKPOINT_COMPLETION_TARGET`
- `ETL_DB_WAL_COMPRESSION`
- `ETL_DB_MAX_PARALLEL_WORKERS`
- `ETL_DB_MAX_PARALLEL_MAINTENANCE_WORKERS`

**Limitations**
- Search and discover are currently very simple and need a lot of polishing.
- No caching layer between the DB and API yet.
- Some data is duplicated for speed.

**Roadmap**
- Refactor monolithic main.go file for cleanliness (very WIP right now)
- Automatic daily update + rebuild
- Better search and discover relevance, scoring, and filtering
- Add a caching layer

<a id="imdb-data-terms-and-attribution"></a>
**IMDb Data Terms and Attribution**
- Use is limited to personal, non-commercial purposes.
- Do not republish/resell/repurpose the data to create a public or commercial database.
- You must comply with IMDb’s Conditions of Use.

```text
Information courtesy of
IMDb
(https://www.imdb.com).
Used with permission.
```
