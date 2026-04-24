"""按角色导出小范围示例数据集。"""

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

from database import close_db, get_session_factory, init_db
from services.dataset import export_character_dataset


async def run(args) -> int:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        for character in args.characters:
            result = await export_character_dataset(
                session=session,
                character_tag=character,
                limit=args.limit,
                rating=args.rating,
                min_score=args.min_score,
                download_images=not args.no_download,
            )
            print(f"{character}: exported={result['exported_count']} dir={result['dataset_dir']} errors={len(result['errors'])}")
        await session.commit()
    await close_db()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--characters", nargs="+", required=True)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--rating")
    parser.add_argument("--min-score", type=int)
    parser.add_argument("--no-download", action="store_true")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
