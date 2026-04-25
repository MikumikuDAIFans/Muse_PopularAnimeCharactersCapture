# Muse Popular Anime Characters Capture / Muse DataLoad

中文 | [English](#english)

Muse DataLoad 是一个基于 Danbooru 内部数据的热门二次元角色发现与训练样本导出工具。它提供 FastAPI 后端、Vue 前端、CLI、榜单生成脚本和校验脚本，用于把 Danbooru 元数据整理成可复核的角色榜单，并按角色导出小批量训练样本。

当前交付重点是两类榜单：

- 最近 6 个月综合热门角色榜，输出 JSON/CSV。
- 新兴热门角色榜，基于首次出现时间、当前年龄和近期活跃度过滤古早常青角色。

后续执行策略已调整为：先抓近两年全站元数据 JSONL，再流式聚合角色候选榜，只对榜单角色做定向样本下载；不做全站图片下载。

## 功能特性

- Danbooru 元数据采集与本地 SQLite 存储。
- 最近 6 个月综合热门角色 Top 200 榜单。
- 新兴角色榜，包含 `first_seen_at`、当前年龄、近期占比和新兴热度分。
- JSON/CSV 榜单导出与校验脚本。
- 按角色导出训练样本：图片、txt caption、json 元数据。
- 图片下载 fallback：原图失败后尝试 large/sample/preview URL。
- Docker 化后端与前端，nginx 代理 `/api`。
- CLI 与脚本入口，便于自动化执行。
- 近两年元数据 JSONL 分片同步、断点续跑和流式候选榜聚合。

## 技术栈

| 层级 | 技术 |
|---|---|
| 后端 | Python 3.12, FastAPI, SQLAlchemy async |
| 数据库 | SQLite |
| 前端 | Vue 3, Vite |
| API 客户端 | Danbooru HTTP API |
| 容器 | Docker Compose |
| 测试 | pytest |

## 快速开始

### Docker

```powershell
# 构建 nginx 挂载的前端 dist。
cd frontend
npm install
npm run build
cd ..

docker-compose up -d --build
docker-compose ps
```

服务地址：

- 前端：<http://localhost:8080>
- 后端健康检查：<http://localhost:8001/health>
- 后端 API 文档：<http://localhost:8001/docs>

Docker Compose 会挂载：

- `./muse_dataload.db` -> `/app/muse_dataload.db`
- `./output` -> `/app/output`

这些运行时文件不会提交到 git。

### 本地开发

后端：

```powershell
python -m venv .venv
.\.venv\Scripts\pip.exe install -r backend\requirements.txt
.\.venv\Scripts\python.exe -m uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

前端：

```powershell
cd frontend
npm install
npm run dev
```

## 常用命令

运行基础校验：

```powershell
.\.venv\Scripts\python.exe -m compileall backend cli scripts tests
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe scripts\smoke_api.py --root http://localhost:8001
```

校验榜单导出：

```powershell
.\.venv\Scripts\python.exe scripts\validate_character_export.py output\exports\character_list_recent_6m_top_200.json output\exports\character_list_recent_6m_top_200.csv --expect-count 200
.\.venv\Scripts\python.exe scripts\validate_emerging_export.py output\exports\character_list_emerging_6m_top_200.json output\exports\character_list_emerging_6m_top_200.csv
```

构建前端：

```powershell
cd frontend
npm run build
```

通过 API 导出单角色样本：

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

校验样本目录：

```powershell
.\.venv\Scripts\python.exe scripts\validate_dataset.py output\dataset\hatsune_miku
```

近两年全站元数据 JSONL 长跑：

```powershell
.\.venv\Scripts\python.exe scripts\sync_recent_posts.py --recent-months 24 --shard monthly --resume --skip-import --task-id-prefix 924000
.\.venv\Scripts\python.exe scripts\build_character_candidates_from_jsonl.py --recent-months 24 --top-n 500 --min-count 50
```

## 核心 API

| 方法 | 路径 | 说明 |
|---|---|---|
| `GET` | `/health` | 后端健康检查 |
| `GET` | `/api/stats` | 系统统计 |
| `GET` | `/api/posts` | 帖子查询 |
| `GET` | `/api/tags` | 标签查询 |
| `GET` | `/api/characters/top` | 综合热门角色榜 |
| `POST` | `/api/characters/build` | 重建综合热门角色榜 |
| `GET` | `/api/characters/emerging` | 新兴热门角色榜 |
| `POST` | `/api/characters/build-emerging` | 重建新兴热门角色榜 |
| `GET` | `/api/export/characters` | 导出综合热门角色榜 |
| `POST` | `/api/datasets/export` | 按角色导出训练样本 |

## 项目结构

```text
Muse_DataLoad/
├── backend/                 # FastAPI 应用、ORM、服务、worker
├── cli/                     # CLI 入口
├── docs/                    # 最终开发计划、验收清单、验收报告
├── frontend/                # Vue 3 前端
├── rules/                   # tag 过滤与 caption 规则
├── scripts/                 # 构建、同步、校验、审计脚本
├── sql/                     # 数据库 SQL 辅助文件
├── tests/                   # pytest 测试
├── docker-compose.yml
├── nginx.conf
├── pytest.ini
└── README.md
```

## 运行时数据

以下内容为本地生成文件，不进入 git：

- `.venv/`
- `frontend/node_modules/`
- `frontend/dist/`
- `muse_dataload.db`
- `output/`
- 日志和测试缓存

当前已验证的本地数据包含 26,400 条帖子和生成后的榜单导出，但这些属于运行时产物，不是源码仓库内容。

## 文档

主要文档：

- [docs/开发计划.md](docs/开发计划.md)
- [docs/验收清单.md](docs/验收清单.md)
- [docs/验收报告.md](docs/验收报告.md)
- [docs/迁移与执行指南.md](docs/迁移与执行指南.md)

## 当前验收快照

最终验收时：

- 后端 Docker 服务在 8001 端口 healthy。
- 前端 Docker 服务在 8080 端口 running。
- 综合榜导出 200 条。
- 新兴榜导出 128 条。
- 随机多角色样本导出回归通过。
- `silver_wolf_(lv.999)_(honkai:_star_rail)` 图片导出 fallback 已验证。

完整证据见 [docs/验收报告.md](docs/验收报告.md)。

---

## English

Muse DataLoad is a Danbooru-based popular anime character discovery and dataset export tool. It provides a FastAPI backend, Vue frontend, CLI helpers, ranking scripts, and validation scripts for turning Danbooru metadata into reviewable character rankings and small per-character training datasets.

The current deliverable focuses on two rankings:

- A recent 6-month comprehensive character ranking, exported as JSON and CSV.
- An emerging character ranking that filters out long-lived evergreen characters using first-seen date, current age, and recent activity.

The next execution strategy is metadata-first: collect recent 24-month site-wide JSONL metadata, build character candidates from JSONL, then download targeted samples only for ranked characters. Site-wide image download is intentionally out of scope.

## Features

- Danbooru metadata ingestion with local SQLite storage.
- Recent 6-month Top 200 comprehensive character ranking.
- Emerging character ranking with `first_seen_at`, current age, recent ratio, and growth score.
- JSON/CSV exports with validation scripts.
- Per-character dataset export: image, txt caption, and json metadata.
- Image download fallback from original URL to large/sample/preview URLs.
- Dockerized backend and frontend with nginx `/api` proxy.
- CLI and script entry points for automation.
- Sharded recent 24-month metadata JSONL sync with resume and streaming candidate aggregation.

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

Run basic validation:

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

Run a recent 24-month metadata-only JSONL sync:

```powershell
.\.venv\Scripts\python.exe scripts\sync_recent_posts.py --recent-months 24 --shard monthly --resume --skip-import --task-id-prefix 924000
.\.venv\Scripts\python.exe scripts\build_character_candidates_from_jsonl.py --recent-months 24 --top-n 500 --min-count 50
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

Primary documentation:

- [docs/开发计划.md](docs/开发计划.md)
- [docs/验收清单.md](docs/验收清单.md)
- [docs/验收报告.md](docs/验收报告.md)
- [docs/迁移与执行指南.md](docs/迁移与执行指南.md)

## Current Acceptance Snapshot

As of the final acceptance pass:

- Backend Docker service is healthy on port 8001.
- Frontend Docker service is running on port 8080.
- Comprehensive ranking export has 200 rows.
- Emerging ranking export has 128 rows.
- Random multi-character dataset export regression passed.
- `silver_wolf_(lv.999)_(honkai:_star_rail)` image export fallback was verified.

See [docs/验收报告.md](docs/验收报告.md) for the full evidence record.
