import pytest
from sqlalchemy import func, select

from config import get_settings
from database import Base, close_db, get_async_engine
from models import JobLog, SyncJob, SyncShard
from services.job_state import sync_manifest_to_db


@pytest.mark.asyncio
async def test_sync_manifest_to_db_records_job_and_shards(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    manifest = {
        "shards": [
            {
                "task_id": 1,
                "tag_filter": "date:2026-01-01..2026-01-31",
                "jsonl": {"exists": True, "lines": 10, "path": "task_1_posts.jsonl"},
                "worker": {"errors": 0},
            },
            {
                "task_id": 2,
                "tag_filter": "date:2026-02-01..2026-02-28",
                "jsonl": {"exists": False, "lines": 0},
                "worker": {"errors": 1},
            },
        ]
    }

    from database import get_session_factory
    factory = await get_session_factory()
    async with factory() as session:
        stats = await sync_manifest_to_db(session, manifest, "job_1")
        await session.commit()
        job = (await session.execute(select(SyncJob).where(SyncJob.job_key == "job_1"))).scalar_one()
        shard_count = (await session.execute(select(func.count()).select_from(SyncShard))).scalar_one()
        log_count = (await session.execute(select(func.count()).select_from(JobLog))).scalar_one()

    assert stats == {"jobs": 1, "shards": 2, "completed_shards": 1, "failed_shards": 1}
    assert job.status == "failed"
    assert shard_count == 2
    assert log_count == 1
    await close_db()
