from datetime import datetime
from pathlib import Path

import pytest

from database import Base, get_async_engine, close_db
from models import Post, PostTag, Tag
from services.ranking import build_character_ranking


@pytest.mark.asyncio
async def test_ranking_exports_rank_and_required_fields(tmp_path, monkeypatch):
    db = tmp_path / "ranking.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db.as_posix()}")
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
