"""补齐角色首次出现时间。"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import func, select

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from models import Character, Post, PostTag, Tag
from services.danbooru import get_danbooru_client
from services.ingest import parse_datetime


async def run(args) -> int:
    if args.database_url:
        os.environ["DATABASE_URL"] = args.database_url
        get_settings.cache_clear()
    await close_db()
    await init_db()
    client = get_danbooru_client()
    now = datetime.utcnow()
    factory = await get_session_factory()
    async with factory() as session:
        # Candidate pool: most active recent character tags, not just current character table.
        from datetime import timedelta
        cutoff = now - timedelta(days=args.recent_months * 30)
        rows = await session.execute(
            select(Tag.id, Tag.name, func.count(func.distinct(PostTag.post_id)).label("recent_count"))
            .join(PostTag, PostTag.tag_id == Tag.id)
            .join(Post, Post.id == PostTag.post_id)
            .where(Tag.category == "character", Post.created_at >= cutoff)
            .group_by(Tag.id)
            .having(func.count(func.distinct(PostTag.post_id)) >= args.min_recent_count)
            .order_by(func.count(func.distinct(PostTag.post_id)).desc())
        )
        candidates = list(rows.all())
        processed = 0
        for tag_id, tag_name, recent_count in candidates:
            if args.limit and processed >= args.limit:
                break
            character_row = await session.execute(select(Character).where(Character.tag_id == int(tag_id)))
            character = character_row.scalar_one_or_none()
            if character is None:
                character = Character(tag_id=int(tag_id))
                session.add(character)
                await session.flush()
            character.recent_post_count = int(recent_count)
            if character.first_seen_at and not args.refresh:
                continue
            post = await asyncio.to_thread(client.get_first_post_for_tag, tag_name)
            if post is None:
                continue
            first_seen_at = parse_datetime(post.created_at)
            character.first_seen_post_id = post.id
            character.first_seen_at = first_seen_at
            if first_seen_at:
                character.character_age_days = max((now - first_seen_at).days, 0)
                character.birth_confidence = 1.0
            character.lifecycle_notes = (character.lifecycle_notes or "")[:1000]
            processed += 1
            if processed % 20 == 0:
                await session.commit()
                print(f"processed={processed}", flush=True)
        await session.commit()
    await close_db()
    print(f"OK enrich birthdates processed={processed}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--min-recent-count", type=int, default=5)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--database-url", help="SQLAlchemy async database URL, e.g. postgresql+asyncpg://...")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
