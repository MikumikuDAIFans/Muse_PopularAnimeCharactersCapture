from sqlalchemy import create_engine, text

from database import Base
import models  # noqa: F401
from scripts.import_jsonl_fast import sync_database_url
from scripts.recount_tag_post_counts import recount


def test_recount_tag_post_counts_updates_tag_table(pg_database_url):
    engine = create_engine(sync_database_url(pg_database_url), future=True)
    with engine.begin() as conn:
        Base.metadata.create_all(conn)
        conn.execute(
            text(
                "insert into tag (id, name, category, post_count, updated_at) "
                "values (1, 'hatsune_miku', 'character', 0, current_timestamp)"
            )
        )
        conn.execute(
            text(
                """
                insert into post (
                    id, tag_count, score, fav_count, has_children,
                    is_deleted, is_flagged, fetched_at, file_verified
                )
                values
                    (100, 0, 0, 0, false, false, false, current_timestamp, false),
                    (101, 0, 0, 0, false, false, false, current_timestamp, false)
                """
            )
        )
        conn.execute(text("insert into post_tag (post_id, tag_id) values (100, 1), (101, 1)"))
    engine.dispose()

    stats = recount(pg_database_url)

    engine = create_engine(sync_database_url(pg_database_url), future=True)
    with engine.connect() as conn:
        post_count = conn.execute(text("select post_count from tag where id = 1")).scalar_one()
    engine.dispose()

    assert stats == {"updated_tags": 1}
    assert post_count == 2
