"""基于 post_tag 全量回填 tag.post_count。"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def recount(db_path: Path) -> dict[str, int]:
    conn = sqlite3.connect(db_path)
    try:
        updated = 0
        rows = conn.execute(
            """
            select tag_id, count(*)
            from post_tag
            group by tag_id
            """
        ).fetchall()
        conn.execute("update tag set post_count = 0, updated_at = current_timestamp")
        for tag_id, observed in rows:
            conn.execute(
                "update tag set post_count = ?, updated_at = current_timestamp where id = ?",
                (int(observed), int(tag_id)),
            )
            updated += 1
        conn.commit()
        return {"updated_tags": updated}
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("muse_dataload.db"))
    args = parser.parse_args()
    stats = recount(args.db)
    print(f"OK recount tag post counts {stats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
