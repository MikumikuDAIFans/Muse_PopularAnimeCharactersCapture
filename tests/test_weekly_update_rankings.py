import argparse

import pytest
from sqlalchemy import select

from config import get_settings
from database import Base, close_db, get_async_engine, get_session_factory
from models import SyncJob
from scripts.weekly_update_rankings import latest_successful_window_end, plan_window, run_key


def make_args(**overrides):
    defaults = {
        "job_key": "weekly-test",
        "start_date": None,
        "end_date": "2026-05-15",
        "lookback_days": 7,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def test_run_key_includes_window():
    assert (
        run_key("weekly", __import__("datetime").date(2026, 5, 1), __import__("datetime").date(2026, 5, 7))
        == "weekly:2026-05-01:2026-05-07"
    )


@pytest.mark.asyncio
async def test_plan_window_uses_explicit_dates(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    args = make_args(start_date="2026-05-01", end_date="2026-05-07")

    start, end = await plan_window(args)

    assert start.isoformat() == "2026-05-01"
    assert end.isoformat() == "2026-05-07"
    await close_db()


@pytest.mark.asyncio
async def test_plan_window_uses_latest_completed_job_watermark(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = await get_session_factory()
    async with factory() as session:
        session.add(
            SyncJob(
                job_key="weekly-test:2026-05-01:2026-05-07",
                job_type="weekly_ranking",
                status="completed",
                params={"window_end": "2026-05-07"},
            )
        )
        await session.commit()

    assert (await latest_successful_window_end("weekly-test")).isoformat() == "2026-05-07"

    start, end = await plan_window(make_args(end_date="2026-05-15"))

    assert start.isoformat() == "2026-05-08"
    assert end.isoformat() == "2026-05-15"

    async with factory() as session:
        jobs = (await session.execute(select(SyncJob))).scalars().all()
    assert len(jobs) == 1
    await close_db()
