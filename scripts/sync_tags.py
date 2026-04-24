"""同步 Danbooru tag 字典、alias 和 implication。"""

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
from services.danbooru import get_danbooru_client
from services.ingest import import_danbooru_aliases, import_danbooru_implications, import_danbooru_tags


def fetch_tags(category: str, limit: int, min_count: int):
    client = get_danbooru_client()
    items = []
    page = 1
    while len(items) < limit:
        batch = client.get_tags(limit=min(1000, limit - len(items)), page=page, category=category, order="count")
        if not batch:
            break
        for item in batch:
            if item.post_count >= min_count:
                items.append(item)
            if len(items) >= limit:
                break
        page += 1
    return items


def fetch_aliases(limit: int):
    client = get_danbooru_client()
    items = []
    page = 1
    while len(items) < limit:
        batch = client.get_tag_aliases(limit=min(1000, limit - len(items)), page=page)
        if not batch:
            break
        items.extend(batch)
        page += 1
    return items[:limit]


def fetch_implications(limit: int):
    client = get_danbooru_client()
    items = []
    page = 1
    while len(items) < limit:
        batch = client.get_tag_implications(limit=min(1000, limit - len(items)), page=page)
        if not batch:
            break
        items.extend(batch)
        page += 1
    return items[:limit]


async def run(args) -> int:
    await init_db()
    tags = await asyncio.to_thread(fetch_tags, args.category, args.limit, args.min_count)
    aliases = await asyncio.to_thread(fetch_aliases, args.alias_limit) if args.alias_limit else []
    implications = await asyncio.to_thread(fetch_implications, args.implication_limit) if args.implication_limit else []
    factory = await get_session_factory()
    async with factory() as session:
        tag_stats = await import_danbooru_tags(session, tags)
        alias_stats = await import_danbooru_aliases(session, aliases)
        implication_stats = await import_danbooru_implications(session, implications)
        await session.commit()
    await close_db()
    print(f"OK sync tags category={args.category} tags={tag_stats['imported']} aliases={alias_stats['imported']} implications={implication_stats['imported']}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", default="character")
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--alias-limit", type=int, default=5000)
    parser.add_argument("--implication-limit", type=int, default=0)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
