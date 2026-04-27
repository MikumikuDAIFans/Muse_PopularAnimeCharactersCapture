"""生成正式角色榜单交付物。"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

from sqlalchemy import text

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from services.ranking import build_character_ranking, recent_cutoff


def configure_database(args) -> None:
    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url
        get_settings.cache_clear()


async def inspect_source(session, recent_months: int) -> dict[str, object]:
    cutoff = recent_cutoff(recent_months)
    post_count = int((await session.execute(text("select count(*) from post"))).scalar() or 0)
    tag_count = int((await session.execute(text("select count(*) from tag"))).scalar() or 0)
    post_tag_count = int((await session.execute(text("select count(*) from post_tag"))).scalar() or 0)
    character_tag_count = int(
        (await session.execute(text("select count(*) from tag where category = 'character'"))).scalar() or 0
    )
    recent_character_tag_count = int(
        (
            await session.execute(
                text(
                    """
                    select count(*) from (
                        select t.id
                        from tag t
                        join post_tag pt on pt.tag_id = t.id
                        join post p on p.id = pt.post_id
                        where t.category = 'character' and p.created_at >= :cutoff
                        group by t.id
                    )
                    """
                ),
                {"cutoff": cutoff},
            )
        ).scalar()
        or 0
    )
    created_range = (
        await session.execute(text("select min(created_at), max(created_at) from post"))
    ).one()
    return {
        "post": post_count,
        "tag": tag_count,
        "post_tag": post_tag_count,
        "character_tags": character_tag_count,
        "recent_character_tags": recent_character_tag_count,
        "recent_window_start": cutoff.isoformat(),
        "post_created_min": created_range[0],
        "post_created_max": created_range[1],
    }


async def run(args) -> int:
    configure_database(args)
    await close_db()
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        source = await inspect_source(session, args.recent_months)
        print(f"source={source}")
        if source["post"] == 0 or source["post_tag"] == 0 or source["character_tags"] == 0:
            raise RuntimeError("ranking source database is empty or missing character tag links")
        if source["recent_character_tags"] == 0:
            raise RuntimeError("ranking source database has no recent character tags for the selected window")
        result = await build_character_ranking(
            session=session,
            output_root=Path(args.output_root or get_settings().OUTPUT_ROOT),
            recent_months=args.recent_months,
            top_n=args.top_n,
            min_post_count=args.min_count,
        )
        await session.commit()
    await close_db()
    print(f"OK build characters total={result['total_count']}")
    print(result["json_path"])
    print(result["csv_path"])
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--output-root")
    parser.add_argument("--database-url", help="SQLAlchemy async database URL, e.g. postgresql+asyncpg://...")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
