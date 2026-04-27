"""重建角色月度聚合表。"""

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
from services.aggregation import rebuild_character_monthly_aggregates


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
        stats = await rebuild_character_monthly_aggregates(session)
        await session.commit()
    await close_db()
    print(f"OK rebuild monthly aggregates {stats}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", help="SQLAlchemy async database URL, e.g. postgresql+asyncpg://...")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
