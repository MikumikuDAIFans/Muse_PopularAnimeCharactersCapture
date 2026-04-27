from sqlalchemy import create_mock_engine

from database import Base
import models  # noqa: F401


def test_metadata_compiles_for_postgresql():
    statements = []
    engine = create_mock_engine(
        "postgresql+psycopg://",
        lambda sql, *multiparams, **params: statements.append(str(sql.compile(dialect=engine.dialect))),
    )

    Base.metadata.create_all(engine)

    ddl = "\n".join(statements)
    assert "CREATE TABLE post" in ddl
    assert "CREATE TABLE sync_job" in ddl
    assert "CREATE TABLE character_monthly_stats" in ddl
    assert "CREATE TABLE ranking_snapshot" in ddl
    assert "CREATE TABLE download_job" in ddl
