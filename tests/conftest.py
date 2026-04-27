from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text


def _admin_url() -> str:
    return os.environ.get(
        "POSTGRES_TEST_ADMIN_URL",
        "postgresql+psycopg://postgres@127.0.0.1:55432/postgres",
    )


def _async_url(db_name: str) -> str:
    base = os.environ.get(
        "POSTGRES_TEST_ASYNC_BASE_URL",
        "postgresql+asyncpg://postgres@127.0.0.1:55432",
    )
    return f"{base}/{db_name}"


@pytest.fixture
def pg_database_url(monkeypatch):
    db_name = f"muse_test_{uuid.uuid4().hex}"
    admin = create_engine(_admin_url(), isolation_level="AUTOCOMMIT", future=True)
    with admin.connect() as conn:
        conn.execute(text(f'create database "{db_name}"'))
    admin.dispose()
    url = _async_url(db_name)
    monkeypatch.setenv("DATABASE_URL", url)
    try:
        yield url
    finally:
        admin = create_engine(_admin_url(), isolation_level="AUTOCOMMIT", future=True)
        with admin.connect() as conn:
            conn.execute(
                text(
                    "select pg_terminate_backend(pid) "
                    "from pg_stat_activity where datname = :db_name and pid <> pg_backend_pid()"
                ),
                {"db_name": db_name},
            )
            conn.execute(text(f'drop database if exists "{db_name}"'))
        admin.dispose()
