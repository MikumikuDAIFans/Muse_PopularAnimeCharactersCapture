"""生成新兴热门角色榜。"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from services.emerging import build_emerging_character_ranking


async def run(args) -> int:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        result = await build_emerging_character_ranking(
            session=session,
            output_root=Path(args.output_root or get_settings().OUTPUT_ROOT),
            top_n=args.top_n,
            min_post_count=args.min_count,
            min_recent_count=args.min_recent_count,
            max_age_days=args.max_age_days,
        )
        await session.commit()
    await close_db()
    print(f"OK build emerging total={result['total_count']}")
    print(result["json_path"])
    print(result["csv_path"])
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--min-recent-count", type=int, default=10)
    parser.add_argument("--max-age-days", type=int, default=1095)
    parser.add_argument("--output-root")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
