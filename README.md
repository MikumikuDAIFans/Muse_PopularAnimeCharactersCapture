# Muse DataLoad

Muse DataLoad is a Danbooru-based character discovery and dataset export tool. It provides a FastAPI backend, Vue frontend, CLI helpers, ranking scripts, and validation scripts for turning Danbooru metadata into curated character rankings and small training sample datasets.

The current deliverable is centered on two rankings:

- A recent 6-month comprehensive character ranking, exported as JSON and CSV.
- An emerging character ranking that filters out long-lived evergreen characters by first-seen date and recent activity.

## Features

- Danbooru metadata ingestion with local SQLite storage.
- Recent 6-month Top 200 comprehensive character ranking.
- Emerging character ranking with `first_seen_at`, current age, recent ratio, and growth score.
- JSON/CSV exports with validation scripts.
- Per-character dataset export: image, txt caption, and json metadata.
- Image download fallback from original URL to large/sample/preview URLs.
- Dockerized backend and frontend with nginx `/api` proxy.
- CLI and script entry points for automation.

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python 3.12, FastAPI, SQLAlchemy async |
| Database | SQLite |
| Frontend | Vue 3, Vite |
| API client | Danbooru HTTP API |
| Container | Docker Compose |
| Tests | pytest |

## Quick Start

### Docker

```powershell
# Build the frontend bundle mounted by the nginx container.
cd frontend
npm install
npm run build
cd ..

docker-compose up -d --build
docker-compose ps
```

Services:

- Frontend: <http://localhost:8080>
- Backend health: <http://localhost:8001/health>
- Backend API docs: <http://localhost:8001/docs>

The compose file mounts:

- `./muse_dataload.db` to `/app/muse_dataload.db`
- `./output` to `/app/output`

These runtime files are intentionally ignored by git.

### Local Development

Backend:

```powershell
python -m venv .venv
.\.venv\Scripts\pip.exe install -r backend\requirements.txt
.\.venv\Scripts\python.exe -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Frontend:

```powershell
cd frontend
npm install
npm run dev
```

## Common Commands

Run validation:

```powershell
.\.venv\Scripts\python.exe -m compileall backend cli scripts tests
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\smoke_api.py --root http://localhost:8001
```

Validate ranking exports:

```powershell
.\.venv\Scripts\python.exe scripts\validate_character_export.py output\exports\character_list_recent_6m_top_200.json output\exports\character_list_recent_6m_top_200.csv --expect-count 200
.\.venv\Scripts\python.exe scripts\validate_emerging_export.py output\exports\character_list_emerging_6m_top_200.json output\exports\character_list_emerging_6m_top_200.csv
```

Build the frontend:

```powershell
cd frontend
npm run build
```

Export a character dataset through the API:

```powershell
$body = @{
  character_tag = "hatsune_miku"
  limit = 5
  download_images = $true
  clean_target_dir = $true
} | ConvertTo-Json

Invoke-RestMethod -Method Post `
  -Uri "http://127.0.0.1:8001/api/datasets/export" `
  -ContentType "application/json" `
  -Body $body
```

Validate a dataset directory:

```powershell
.\.venv\Scripts\python.exe scripts\validate_dataset.py output\dataset\hatsune_miku
```

## API Highlights

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Backend health check |
| `GET` | `/api/stats` | System statistics |
| `GET` | `/api/posts` | Post search/list |
| `GET` | `/api/tags` | Tag search/list |
| `GET` | `/api/characters/top` | Comprehensive character ranking |
| `POST` | `/api/characters/build` | Rebuild comprehensive ranking |
| `GET` | `/api/characters/emerging` | Emerging character ranking |
| `POST` | `/api/characters/build-emerging` | Rebuild emerging ranking |
| `GET` | `/api/export/characters` | Export comprehensive ranking |
| `POST` | `/api/datasets/export` | Export per-character dataset |

## Project Layout

```text
Muse_DataLoad/
├── backend/                 # FastAPI app, ORM models, services, workers
├── cli/                     # CLI entry point
├── docs/                    # Final plan, checklist, and acceptance report
├── frontend/                # Vue 3 frontend
├── rules/                   # Tag filtering and caption rules
├── scripts/                 # Build, sync, validation, and audit scripts
├── sql/                     # Database SQL helpers
├── tests/                   # pytest suite
├── docker-compose.yml
├── nginx.conf
├── pytest.ini
└── README.md
```

## Runtime Data

The following are generated locally and are not committed:

- `.venv/`
- `frontend/node_modules/`
- `frontend/dist/`
- `muse_dataload.db`
- `output/`
- logs and test caches

The current verified local dataset includes 26,400 posts and generated ranking exports, but those files are runtime artifacts rather than source-controlled project files.

## Documentation

The project documentation has been consolidated into three final files:

- [docs/开发计划.md](docs/开发计划.md)
- [docs/验收清单.md](docs/验收清单.md)
- [docs/验收报告.md](docs/验收报告.md)

## Current Acceptance Snapshot

As of the final acceptance pass:

- Backend Docker service is healthy on port 8001.
- Frontend Docker service is running on port 8080.
- Comprehensive ranking export has 200 rows.
- Emerging ranking export has 128 rows.
- Random multi-character dataset export regression passed.
- `silver_wolf_(lv.999)_(honkai:_star_rail)` image export fallback was verified.

See [docs/验收报告.md](docs/验收报告.md) for the full evidence record.
