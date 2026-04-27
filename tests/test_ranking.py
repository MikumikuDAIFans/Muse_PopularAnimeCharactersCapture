from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import func, select

from config import get_settings
from database import Base, get_async_engine, close_db
from models import Character, RankingSnapshot, RankingSnapshotItem, Post, PostTag, Tag
from services.aggregation import rebuild_character_monthly_aggregates
from services.emerging import build_emerging_character_ranking
from services.ranking import build_character_ranking


@pytest.mark.asyncio
async def test_ranking_exports_rank_and_required_fields(pg_database_url, tmp_path):
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
        snapshot_count = (
            await session.execute(select(func.count()).select_from(RankingSnapshot))
        ).scalar_one()
        snapshot_item_count = (
            await session.execute(select(func.count()).select_from(RankingSnapshotItem))
        ).scalar_one()

    assert result["total_count"] == 1
    item = result["characters"][0]
    assert item["rank"] == 1
    assert item["character_tag"] == "hatsune_miku"
    assert item["copyrights"] == ["vocaloid"]
    assert Path(result["json_path"]).exists()
    assert Path(result["csv_path"]).exists()
    assert Path(result["json_path"]).name == "character_list_recent_6m_top_10.json"
    assert Path(result["csv_path"]).name == "character_list_recent_6m_top_10.csv"
    assert snapshot_count == 1
    assert snapshot_item_count == 1
    await close_db()


@pytest.mark.asyncio
async def test_ranking_uses_observed_post_counts_when_tag_post_count_is_zero(pg_database_url, tmp_path):
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


@pytest.mark.asyncio
async def test_emerging_ranking_uses_database_backed_recent_candidates(pg_database_url, tmp_path):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        char = Tag(name="fresh_character", category="character", post_count=10)
        cr = Tag(name="fresh_series", category="copyright", post_count=10)
        session.add_all([char, cr])
        await session.flush()
        post = Post(
            id=500,
            tag_string="fresh_character fresh_series",
            created_at=datetime.utcnow(),
        )
        session.add(post)
        await session.flush()
        session.add_all([
            PostTag(post_id=post.id, tag_id=char.id),
            PostTag(post_id=post.id, tag_id=cr.id),
            Character(
                tag_id=char.id,
                total_post_count=10,
                recent_post_count=1,
                popularity_score=0.5,
                first_seen_post_id=post.id,
                first_seen_at=post.created_at,
                birth_confidence=1.0,
            ),
        ])

        result = await build_emerging_character_ranking(
            session,
            tmp_path,
            min_post_count=1,
            min_recent_count=1,
            max_age_days=1095,
        )
        await session.commit()
        snapshot_count = (
            await session.execute(select(func.count()).select_from(RankingSnapshot))
        ).scalar_one()
        snapshot_item_count = (
            await session.execute(select(func.count()).select_from(RankingSnapshotItem))
        ).scalar_one()

    assert result["total_count"] == 1
    assert result["characters"][0]["character_tag"] == "fresh_character"
    assert result["characters"][0]["copyrights"] == ["fresh_series"]
    assert snapshot_count == 1
    assert snapshot_item_count == 1
    await close_db()


@pytest.mark.asyncio
async def test_rebuild_character_monthly_aggregates(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        char = Tag(name="monthly_character", category="character", post_count=2)
        cr = Tag(name="monthly_series", category="copyright", post_count=2)
        session.add_all([char, cr])
        await session.flush()
        post = Post(
            id=700,
            tag_string="monthly_character monthly_series",
            fav_count=3,
            score=4,
            created_at=datetime(2026, 1, 10),
        )
        session.add(post)
        await session.flush()
        session.add_all([
            PostTag(post_id=post.id, tag_id=char.id),
            PostTag(post_id=post.id, tag_id=cr.id),
        ])
        stats = await rebuild_character_monthly_aggregates(session)
        await session.commit()

    assert stats == {
        "character_monthly_stats": 1,
        "character_monthly_copyright": 1,
    }
    await close_db()
