"""比较不同年龄阈值下的新兴榜结果。"""

from __future__ import annotations

import argparse
import asyncio
import json
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
    report = []
    async with factory() as session:
        for age in args.age_days:
            result = await build_emerging_character_ranking(
                session=session,
                output_root=Path(args.output_root or get_settings().OUTPUT_ROOT),
                top_n=args.top_n,
                min_post_count=args.min_count,
                min_recent_count=args.min_recent_count,
                max_age_days=age,
            )
            top_names = [c["character_tag"] for c in result["characters"][:10]]
            report.append({"max_age_days": age, "count": result["total_count"], "top10": top_names})
            print(f"age={age} count={result['total_count']} top1={top_names[0] if top_names else None}")
    await close_db()
    out = Path(args.output or "output/reports/emerging_profile_compare.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(out)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--age-days", nargs="+", type=int, default=[730, 1095, 1460])
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--min-recent-count", type=int, default=10)
    parser.add_argument("--output")
    parser.add_argument("--output-root")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
