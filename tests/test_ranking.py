from datetime import datetime
from pathlib import Path

import pytest

from config import get_settings
from database import Base, get_async_engine, close_db
from models import Post, PostTag, Tag
from services.ranking import build_character_ranking


@pytest.mark.asyncio
async def test_ranking_exports_rank_and_required_fields(tmp_path, monkeypatch):
    db = tmp_path / "ranking.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db.as_posix()}")
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        char = Tag(name="hatsune_miku", category="character", post_count=1000)
        cr = Tag(name="vocaloid", category="copyright", post_count=1000)
        general = Tag(name="1girl", category="general", post_count=1000)
        session.add_all([char, cr, general])
        await session.flush()
        post = Post(
            id=1,
            tag_string="1girl hatsune_miku vocaloid",
            score=10,
            fav_count=5,
            created_at=datetime.utcnow(),
        )
        session.add(post)
        await session.flush()
        session.add_all([
            PostTag(post_id=1, tag_id=char.id),
            PostTag(post_id=1, tag_id=cr.id),
            PostTag(post_id=1, tag_id=general.id),
        ])
        result = await build_character_ranking(session, tmp_path, min_post_count=1, top_n=10)
        await session.commit()

    assert result["total_count"] == 1
    item = result["characters"][0]
    assert item["rank"] == 1
    assert item["character_tag"] == "hatsune_miku"
    assert item["copyrights"] == ["vocaloid"]
    assert Path(result["json_path"]).exists()
    assert Path(result["csv_path"]).exists()
    await close_db()


@pytest.mark.asyncio
async def test_ranking_uses_observed_post_counts_when_tag_post_count_is_zero(tmp_path, monkeypatch):
    db = tmp_path / "ranking_local_counts.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db.as_posix()}")
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        char = Tag(name="new_character", category="character", post_count=0)
        cr = Tag(name="new_series", category="copyright", post_count=0)
        session.add_all([char, cr])
        await session.flush()
        posts = []
        for post_id in range(101, 104):
            posts.append(
                Post(
                    id=post_id,
                    tag_string="new_character new_series",
                    created_at=datetime.utcnow(),
                )
            )
        session.add_all(posts)
        await session.flush()
        for post in posts:
            session.add_all([
                PostTag(post_id=post.id, tag_id=char.id),
                PostTag(post_id=post.id, tag_id=cr.id),
            ])
        result = await build_character_ranking(session, tmp_path, min_post_count=1, top_n=10)
        await session.commit()

    assert result["total_count"] == 1
    assert result["characters"][0]["character_tag"] == "new_character"
    assert result["characters"][0]["post_count"] == 3
    await close_db()
