"""基于 post_tag 全量回填 PostgreSQL tag.post_count。"""

from __future__ import annotations

import argparse
import os
import sys

from sqlalchemy import create_engine, func, select, update

ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from scripts.import_jsonl_fast import sync_database_url
from database import Base
import models  # noqa: F401


def recount(database_url: str) -> dict[str, int]:
    engine = create_engine(sync_database_url(database_url), future=True)
    tag_table = Base.metadata.tables["tag"]
    post_tag_table = Base.metadata.tables["post_tag"]
    with engine.begin() as conn:
        conn.execute(update(tag_table).values(post_count=0))
        rows = conn.execute(select(post_tag_table.c.tag_id, func.count()).group_by(post_tag_table.c.tag_id)).all()
        updated = 0
        for tag_id, observed in rows:
            conn.execute(
                update(tag_table)
                .where(tag_table.c.id == int(tag_id))
                .values(post_count=int(observed))
            )
            updated += 1
    engine.dispose()
    return {"updated_tags": updated}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), required=os.environ.get("DATABASE_URL") is None)
    args = parser.parse_args()
    stats = recount(args.database_url)
    print(f"OK recount tag post counts {stats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
