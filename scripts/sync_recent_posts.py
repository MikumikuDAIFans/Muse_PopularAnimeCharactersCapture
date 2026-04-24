"""同步最近时间窗口的 Danbooru 帖子元数据。

默认只同步元数据，不下载图片。全量最近半年可能耗时很长，建议先用
`--limit 1000` 灰度确认，再移除 limit 长跑。
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from services.ingest import import_jsonl
from workers.crawler import PostCrawlerWorker


async def run(args) -> int:
    cutoff = datetime.utcnow() - timedelta(days=args.recent_months * 30)
    tag_filter = args.tag_filter or f"date:{cutoff.date().isoformat()}.."
    output_root = Path(args.output_root or get_settings().OUTPUT_ROOT)

    await init_db()
    worker = PostCrawlerWorker(
        task_id=args.task_id,
        output_dir=output_root / "metadata",
        tag_filter=tag_filter,
        tags=args.tags or [],
        limit=args.limit,
    )
    worker.run()
    factory = await get_session_factory()
    async with factory() as session:
        stats = await import_jsonl(session, Path(worker.result["output_file"]), args.task_id)
        await session.commit()
    await close_db()
    print(f"OK sync recent tag_filter={tag_filter}")
    print(f"worker={worker.result}")
    print(f"ingest={stats}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--tag-filter")
    parser.add_argument("--tags", nargs="*")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--task-id", type=int, default=900001)
    parser.add_argument("--output-root")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
