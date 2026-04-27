"""把同步 manifest 导入正式任务状态表。"""

from __future__ import annotations

import argparse
import asyncio
import json
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
from services.job_state import sync_manifest_to_db


def configure_database(args) -> None:
    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url
        get_settings.cache_clear()


async def run(args) -> int:
    configure_database(args)
    await close_db()
    await init_db()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    job_key = args.job_key or args.manifest.stem
    factory = await get_session_factory()
    async with factory() as session:
        stats = await sync_manifest_to_db(session, manifest, job_key, args.manifest)
        await session.commit()
    await close_db()
    print(f"OK sync manifest {stats}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--job-key")
    parser.add_argument("--database-url", help="SQLAlchemy async database URL, e.g. postgresql+asyncpg://...")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
