"""从榜单快照创建定向下载任务，不下载图片。"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from services.download_jobs import create_download_job_from_snapshot


def configure_database(args) -> None:
    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url
        get_settings.cache_clear()


async def run(args) -> int:
    configure_database(args)
    await close_db()
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        stats = await create_download_job_from_snapshot(
            session=session,
            ranking_type=args.ranking_type,
            name=args.name,
            target_count=args.target_count,
            snapshot_id=args.snapshot_id,
        )
        await session.commit()
    await close_db()
    print(f"OK create download job {stats}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ranking-type", default="recent")
    parser.add_argument("--snapshot-id", type=int)
    parser.add_argument("--name", default="ranking-directed-sample")
    parser.add_argument("--target-count", type=int, default=20)
    parser.add_argument("--database-url", help="SQLAlchemy async database URL, e.g. postgresql+asyncpg://...")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
