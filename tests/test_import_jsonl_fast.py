import json
import sqlite3
from pathlib import Path

from scripts.import_jsonl_fast import ensure_schema, import_file


def test_ensure_schema_bootstraps_empty_sqlite_db(tmp_path):
    db_path = tmp_path / "bootstrap.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)
        tables = {
            row[0] for row in conn.execute("select name from sqlite_master where type='table'").fetchall()
        }
    finally:
        conn.close()

    assert "post" in tables
    assert "tag" in tables
    assert "post_tag" in tables


def test_import_file_populates_tags_and_post_links(tmp_path):
    db_path = tmp_path / "import.db"
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

    stats = import_file(db_path, jsonl_path, None, 1000, False)

    conn = sqlite3.connect(db_path)
    try:
        tag_rows = conn.execute("select name, category from tag order by name").fetchall()
        post_tag_count = conn.execute("select count(*) from post_tag").fetchone()[0]
    finally:
        conn.close()

    assert stats == {"imported": 1, "errors": 0}
    assert tag_rows == [("1girl", "general"), ("hatsune_miku", "character"), ("vocaloid", "copyright")]
    assert post_tag_count == 3
