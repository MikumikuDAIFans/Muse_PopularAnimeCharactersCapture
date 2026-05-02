# Muse_DataLoad 项目结构

Danbooru 动漫角色训练数据采集与管理平台。

**技术栈**: FastAPI + SQLAlchemy (async) + PostgreSQL 16 + Vue 3 + Vite + Docker

---

## 根目录

```
Muse_DataLoad/
├── backend/                # FastAPI 后端服务
├── frontend/               # Vue 3 前端应用
├── cli/                    # 命令行工具
├── scripts/                # 数据管线脚本
├── tests/                  # 测试套件
├── rules/                  # YAML 标签清洗规则
├── sql/                    # 数据库初始化 SQL
├── docs/                   # 项目文档
├── deploy/                 # 远程部署脚本 (gitignored)
├── output/                 # 运行时输出 (gitignored)
├── output_pg/              # PG 相关输出 (gitignored)
├── docker-compose.yml      # 容器编排 (PostgreSQL + Backend + Frontend)
├── nginx.conf              # Nginx 反向代理配置
├── pytest.ini              # pytest 配置
├── README.md               # 项目说明
├── .gitignore
├── .gitattributes
└── .dockerignore
```

---

## backend/ — FastAPI 后端

```
backend/
├── main.py                 # 应用入口 (lifespan, CORS, WebSocket, SPA fallback)
├── config.py               # Pydantic Settings 配置管理
├── database.py             # SQLAlchemy 异步引擎 & Session 工厂
├── models.py               # ORM 模型 (20+ 张表)
├── schemas.py              # Pydantic 请求/响应模型
├── requirements.txt        # Python 依赖
├── Dockerfile              # 容器构建文件
├── .env.example            # 环境变量模板
├── api/
│   ├── routes.py           # REST API 路由定义
│   └── deps.py             # FastAPI 依赖注入
├── services/               # 业务逻辑层
│   ├── danbooru.py         # Danbooru API 客户端
│   ├── ingest.py           # 数据入库
│   ├── aggregation.py      # 聚合统计
│   ├── ranking.py          # 角色排名算法
│   ├── emerging.py         # 新兴角色排名
│   ├── dataset.py          # 训练数据集导出
│   ├── download_jobs.py    # 下载任务管理
│   ├── job_state.py        # 任务状态机
│   ├── rules.py            # 标签清洗规则引擎
│   └── runner.py           # 后台任务执行器
├── workers/                # 爬虫 & 分析 Worker
│   ├── crawler.py          # 帖子爬取
│   ├── analyzer.py         # 数据分析
│   └── base.py             # Worker 基类
└── utils/
    ├── rate_limit.py       # 速率限制
    └── logging.py          # 日志工具
```

### 数据库模型 (models.py)

| 表 | 说明 |
|---|---|
| `project` | 项目 |
| `task` / `task_log` | 爬取任务 & 日志 |
| `post` / `post_tag` | 帖子元数据 & 标签关联 |
| `tag` / `tag_alias` / `tag_implication` | 标签体系 |
| `character` | 角色 (热度、成长、生命周期) |
| `copyright` / `character_copyright` | 作品 & 角色-作品关联 |
| `character_monthly_stats` / `character_monthly_copyright` | 月度聚合 |
| `character_birth_cache` | 角色首现缓存 |
| `ranking_snapshot` / `ranking_snapshot_item` | 榜单快照 |
| `download_job` / `download_job_item` | 下载任务 |
| `download_log` | 下载日志 |
| `sync_job` / `sync_shard` / `sync_checkpoint` | 可恢复同步 |
| `job_log` | 任务运行日志 |
| `system_stats` | 系统统计 |

### API 端点 (api/routes.py)

| 分组 | 端点 |
|---|---|
| 项目 | `GET/POST /api/projects`, `GET/DELETE /api/projects/{id}` |
| 任务 | `GET/POST /api/tasks`, `GET/DELETE /api/tasks/{id}`, `POST .../start\|pause\|stop`, `GET .../logs` |
| 帖子 | `GET /api/posts`, `GET /api/posts/stats`, `GET /api/posts/{id}` |
| 标签 | `GET /api/tags`, `GET /api/tags/character` |
| 角色 | `GET /api/characters`, `GET /api/characters/top`, `GET /api/characters/emerging`, `POST /api/characters/build`, `POST /api/characters/build-emerging` |
| 导出 | `GET /api/export/characters`, `POST /api/datasets/export` |
| 统计 | `GET /api/stats` |
| WebSocket | `/ws/progress/{task_id}`, `/ws/logs/{task_id}` |

---

## frontend/ — Vue 3 前端

```
frontend/
├── src/
│   ├── App.vue             # 根组件
│   ├── main.js             # 入口
│   ├── api/client.js       # API 客户端封装
│   └── pages/
│       ├── Dashboard.vue   # 仪表盘
│       ├── DataPage.vue    # 数据浏览
│       └── TaskPage.vue    # 任务管理
├── index.html
├── vite.config.js
└── package.json
```

---

## scripts/ — 数据管线脚本

```
scripts/
├── import_jsonl_fast.py            # JSONL 快速导入 PostgreSQL
├── sync_tags.py                    # Danbooru 标签同步
├── sync_recent_posts.py            # 近期帖子同步
├── sync_manifest_to_db.py          # 清单文件同步到 DB
├── build_character_list.py         # 角色列表构建
├── build_emerging_character_list.py # 新兴角色列表构建
├── build_character_candidates_from_jsonl.py
├── rebuild_character_monthly_stats.py # 角色月度统计重建
├── recount_tag_post_counts.py      # 标签帖子数重算
├── enrich_character_birthdates.py  # 角色生日信息补全
├── create_download_job_from_snapshot.py # 从快照创建下载任务
├── export_sample_dataset.py        # 样本数据集导出
├── retry_failed_shards.py          # 失败分片重试
├── smoke_api.py                    # API 冒烟测试
├── analyze_thresholds.py           # 阈值分析
├── validate_character_export.py    # 角色导出校验
├── validate_dataset.py             # 数据集校验
├── validate_emerging_export.py     # 新兴角色导出校验
├── audit_copyright_coverage.py     # 版权覆盖审计
├── compare_emerging_profiles.py    # 新兴角色画像对比
└── legacy/
    └── danbooru_git.py             # 早期独立采集脚本 (已被 backend 取代)
```

---

## tests/ — 测试

```
tests/
├── conftest.py                     # pytest fixtures
├── test_ranking.py                 # 角色排名测试
├── test_rules.py                   # 标签规则测试
├── test_download_jobs.py           # 下载任务测试
├── test_job_state.py               # 任务状态测试
├── test_import_jsonl_fast.py       # JSONL 导入测试
├── test_metadata_sync.py           # 元数据同步测试
├── test_postgres_schema.py         # PG Schema 测试
├── test_recount_tag_post_counts.py # 标签重算测试
└── test_caption_order.py           # Caption 排序测试
```

---

## rules/ — 标签清洗规则

```
rules/
├── character_filter.yml            # 角色标签过滤
├── ambiguous_character_tags.yml    # 歧义角色标签处理
├── alias_overrides.yml             # 别名覆盖
├── subject_tags.yml                # 主题标签
└── tag_cleaning.yml                # 标签清洗规则
```

---

## docs/ — 文档

```
docs/
├── 开发计划.md                      # 开发计划
├── 迁移与执行指南.md                # 迁移指南
├── 验收报告.md                      # 验收报告
├── 验收清单.md                      # 验收清单
├── Muse_Intern_Task_Danbooru.md    # 原始需求文档
└── PROJECT_STRUCTURE.md            # 本文件
```

---

## deploy/ — 远程部署脚本 (gitignored)

```
deploy/
├── remote_setup_pg.ps1             # 远程 PostgreSQL 初始化
├── remote_setup_pg_debug.ps1       # PG 调试模式启动
├── remote_check_pg_bins.ps1        # PG 二进制文件检查
├── remote_check_db.py              # 数据库状态检查
├── remote_kill_old.ps1             # 终止旧进程
├── remote_start_import_24m.ps1     # 启动 2400 万条数据导入
├── remote_run_import_24m.ps1       # 执行导入
└── remote_run_check_db.ps1         # 执行数据库检查
```

---

## 关键数据流

```
Danbooru API
    │
    ▼
sync_tags / sync_recent_posts / import_jsonl_fast   (scripts/)
    │
    ▼
PostgreSQL  (models.py: post, tag, character, ...)
    │
    ▼
rebuild_character_monthly_stats → ranking / emerging  (services/)
    │
    ▼
ranking_snapshot → export JSON/CSV                    (output/exports/)
    │
    ▼
dataset.py → 训练数据集 (图片 + caption + metadata)   (output/dataset/)
```

---

## 启动方式

```bash
# Docker 一键启动
docker-compose up -d

# 本地开发
cd backend && pip install -r requirements.txt && python -m backend.main
cd frontend && npm install && npm run dev
```
