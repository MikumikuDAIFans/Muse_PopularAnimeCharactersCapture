from datetime import datetime

import pytest
from sqlalchemy import func, select

from config import get_settings
from database import Base, close_db, get_async_engine
from models import DownloadJob, DownloadJobItem, RankingSnapshot, RankingSnapshotItem, Tag
from services.download_jobs import create_download_job_from_snapshot


@pytest.mark.asyncio
async def test_create_download_job_from_snapshot(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        tag = Tag(name="download_character", category="character", post_count=100)
        session.add(tag)
        await session.flush()
        snapshot = RankingSnapshot(
            ranking_type="recent",
            generated_at=datetime.utcnow(),
            filters={},
        )
        session.add(snapshot)
        await session.flush()
        session.add(
            RankingSnapshotItem(
                snapshot_id=snapshot.id,
                rank=1,
                character_tag_id=tag.id,
                character_tag=tag.name,
                post_count=100,
                recent_post_count=10,
                popularity_score=1.0,
                payload={},
            )
        )
        stats = await create_download_job_from_snapshot(session, "recent", "sample", 20)
        await session.commit()
        job_count = (await session.execute(select(func.count()).select_from(DownloadJob))).scalar_one()
        item = (await session.execute(select(DownloadJobItem))).scalar_one()

    assert stats["items"] == 1
    assert job_count == 1
    assert item.character_tag == "download_character"
    assert item.target_count == 20
    await close_db()
