import pytest

from config import get_settings
from database import Base, close_db, get_async_engine
from scripts.collect_performance_baseline import collect


class Args:
    database_url = None
    snapshot_limit = 5
    explain = False


@pytest.mark.asyncio
async def test_collect_performance_baseline_reads_empty_database(pg_database_url):
    get_settings.cache_clear()
    await close_db()
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    payload = await collect(Args())

    assert payload["table_counts"]["post"] == 0
    assert payload["table_counts"]["ranking_snapshot"] == 0
    assert payload["latest_snapshots"] == []
    assert payload["explain_plans"] == {}
    await close_db()
