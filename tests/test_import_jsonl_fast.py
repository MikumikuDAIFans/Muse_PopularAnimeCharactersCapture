import json

from sqlalchemy import create_engine, text

from scripts.import_jsonl_fast import import_file, sync_database_url


def test_sync_database_url_converts_postgres_drivers():
    assert (
        sync_database_url("postgresql+asyncpg://u:p@localhost/db")
        == "postgresql+psycopg://u:p@localhost/db"
    )
    assert sync_database_url("postgresql://u:p@localhost/db") == "postgresql+psycopg://u:p@localhost/db"


def test_import_file_populates_postgresql_tables(pg_database_url, tmp_path):
    jsonl_path = tmp_path / "sample.jsonl"
    jsonl_path.write_text(
        json.dumps(
            {
                "id": 1,
                "tag_string_character": "hatsune_miku",
                "tag_string_copyright": "vocaloid",
                "tag_string_general": "1girl",
                "created_at": "2026-01-01T00:00:00Z",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    stats = import_file(pg_database_url, jsonl_path, None, 1000, True)

    engine = create_engine(sync_database_url(pg_database_url), future=True)
    with engine.connect() as conn:
        tag_rows = conn.execute(text("select name, category, post_count from tag order by name")).fetchall()
        post_tag_count = conn.execute(text("select count(*) from post_tag")).scalar_one()
    engine.dispose()

    assert stats == {"imported": 1, "errors": 0}
    assert tag_rows == [
        ("1girl", "general", 1),
        ("hatsune_miku", "character", 1),
        ("vocaloid", "copyright", 1),
    ]
    assert post_tag_count == 3
