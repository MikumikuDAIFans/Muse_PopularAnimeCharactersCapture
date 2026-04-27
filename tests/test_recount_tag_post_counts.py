import sqlite3
from pathlib import Path

from scripts.import_jsonl_fast import ensure_schema
from scripts.recount_tag_post_counts import recount


def test_recount_tag_post_counts_updates_tag_table(tmp_path):
    db_path = tmp_path / "recount.db"
    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)
        conn.execute("insert into tag (id, name, category, post_count) values (1, 'hatsune_miku', 'character', 0)")
        conn.execute("insert into post (id) values (100)")
        conn.execute("insert into post (id) values (101)")
        conn.execute("insert into post_tag (post_id, tag_id) values (100, 1)")
        conn.execute("insert into post_tag (post_id, tag_id) values (101, 1)")
        conn.commit()
    finally:
        conn.close()

    stats = recount(db_path)

    conn = sqlite3.connect(db_path)
    try:
        post_count = conn.execute("select post_count from tag where id = 1").fetchone()[0]
    finally:
        conn.close()

    assert stats == {"updated_tags": 1}
    assert post_count == 2
