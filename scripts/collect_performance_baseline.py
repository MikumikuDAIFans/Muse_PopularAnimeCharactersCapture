"""采集 Muse DataLoad PostgreSQL 性能基线。

该脚本不修改业务数据，只读取核心表行数、最近榜单快照和关键查询计划。
如需采集导入/聚合/榜单命令耗时，应在正式执行时把命令输出追加到
docs/验收报告.md；本脚本负责提供稳定的数据规模和 SQL 计划入口。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory


TABLES = [
    "post",
    "tag",
    "post_tag",
    "character",
    "character_monthly_stats",
    "character_monthly_copyright",
    "ranking_snapshot",
    "ranking_snapshot_item",
    "sync_job",
    "sync_shard",
    "job_log",
]


PLAN_QUERIES = {
    "tag_recount": """
        select tag_id, count(*)::integer as count
        from post_tag
        group by tag_id
    """,
    "monthly_character_stats": """
        select
            t.id as character_tag_id,
            date_trunc('month', p.created_at)::date as month_start,
            count(distinct p.id) as post_count
        from tag t
        join post_tag pt on pt.tag_id = t.id
        join post p on p.id = pt.post_id
        where t.category = 'character' and p.created_at is not null
        group by t.id, date_trunc('month', p.created_at)::date
    """,
    "recent_character_candidates": """
        select t.id, t.name, t.post_count, count(distinct pt.post_id) as recent_count
        from tag t
        join post_tag pt on pt.tag_id = t.id
        join post p on p.id = pt.post_id
        where t.category = 'character'
          and p.created_at >= current_timestamp - interval '180 days'
        group by t.id
        order by recent_count desc
        limit 200
    """,
}


def configure_database(database_url: str | None) -> None:
    if database_url:
        os.environ["DATABASE_URL"] = database_url
        get_settings.cache_clear()


async def scalar(session, sql: str, params: dict[str, Any] | None = None) -> Any:
    return (await session.execute(text(sql), params or {})).scalar()


async def collect(args: argparse.Namespace) -> dict[str, Any]:
    configure_database(args.database_url)
    await close_db()
    factory = await get_session_factory()
    async with factory() as session:
        table_counts = {
            table: int(await scalar(session, f"select count(*) from {table}") or 0)
            for table in TABLES
        }
        post_range = (
            await session.execute(text("select min(created_at), max(created_at) from post"))
        ).one()
        latest_snapshots = [
            {
                "id": row[0],
                "ranking_type": row[1],
                "generated_at": row[2].isoformat() if row[2] else None,
                "export_json_path": row[3],
                "export_csv_path": row[4],
            }
            for row in (
                await session.execute(
                    text(
                        """
                        select id, ranking_type, generated_at, export_json_path, export_csv_path
                        from ranking_snapshot
                        order by generated_at desc
                        limit :limit
                        """
                    ),
                    {"limit": args.snapshot_limit},
                )
            ).all()
        ]
        plans: dict[str, list[str]] = {}
        if args.explain:
            for name, query in PLAN_QUERIES.items():
                rows = await session.execute(text(f"explain (analyze, buffers, format text) {query}"))
                plans[name] = [str(row[0]) for row in rows.all()]
    await close_db()
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "database_url": args.database_url or os.environ.get("DATABASE_URL"),
        "table_counts": table_counts,
        "post_created_at_min": post_range[0].isoformat() if post_range[0] else None,
        "post_created_at_max": post_range[1].isoformat() if post_range[1] else None,
        "latest_snapshots": latest_snapshots,
        "explain_plans": plans,
    }


async def run(args: argparse.Namespace) -> int:
    payload = await collect(args)
    output = args.output
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"OK baseline report {output}")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", help="SQLAlchemy async database URL")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--snapshot-limit", type=int, default=5)
    parser.add_argument("--explain", action="store_true", help="Run EXPLAIN ANALYZE for key read queries")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
