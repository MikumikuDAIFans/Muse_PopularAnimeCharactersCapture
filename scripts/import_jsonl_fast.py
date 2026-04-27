"""快速导入 Danbooru JSONL 元数据到 PostgreSQL。"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, delete, func, select, update

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from database import Base
import models  # noqa: F401


TAG_FIELDS = {
    "general": "tag_string_general",
    "character": "tag_string_character",
    "copyright": "tag_string_copyright",
    "artist": "tag_string_artist",
    "meta": "tag_string_meta",
}


def split_tags(value: str | None) -> list[str]:
    return [t.strip() for t in (value or "").split() if t.strip()]


def parse_datetime_value(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def grouped_tags(post: dict[str, Any]) -> dict[str, list[str]]:
    return {category: split_tags(post.get(field)) for category, field in TAG_FIELDS.items()}


def sync_database_url(url: str) -> str:
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("postgresql+psycopg://"):
        return url
    raise ValueError("PostgreSQL DATABASE_URL is required, e.g. postgresql+asyncpg://user:pass@host/db")


def build_post_values(post: dict[str, Any], task_id: int | None) -> dict[str, Any]:
    return {
        "id": int(post["id"]),
        "task_id": task_id,
        "md5": post.get("md5"),
        "file_url": post.get("file_url"),
        "preview_url": post.get("preview_url"),
        "sample_url": post.get("sample_url"),
        "source": post.get("source"),
        "uploader_id": post.get("uploader_id"),
        "uploader_name": post.get("uploader_name"),
        "tag_string": post.get("tag_string"),
        "tag_count": int(post.get("tag_count") or 0),
        "file_ext": post.get("file_ext"),
        "file_size": post.get("file_size"),
        "image_width": post.get("image_width"),
        "image_height": post.get("image_height"),
        "score": int(post.get("score") or 0),
        "fav_count": int(post.get("fav_count") or 0),
        "rating": post.get("rating"),
        "sources": post.get("sources") or [],
        "has_children": bool(post.get("has_children", False)),
        "is_deleted": bool(post.get("is_deleted", False)),
        "is_flagged": bool(post.get("is_flagged", False)),
        "created_at": parse_datetime_value(post.get("created_at")),
        "file_verified": False,
    }


def post_upsert(table, values: dict[str, Any], update_columns: list[str]):
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(table).values(**values)
    return stmt.on_conflict_do_update(
        index_elements=[table.c.id],
        set_={name: getattr(stmt.excluded, name) for name in update_columns},
    )


def insert_ignore(table, values: dict[str, Any]):
    from sqlalchemy.dialects.postgresql import insert

    return insert(table).values(**values).on_conflict_do_nothing()


def resolve_tag_id(conn, tag_table, cache: dict[tuple[str, str], int], name: str, category: str) -> int:
    key = (name, category)
    if key in cache:
        return cache[key]
    row = conn.execute(select(tag_table.c.id, tag_table.c.category).where(tag_table.c.name == name)).first()
    if row:
        tag_id = int(row.id)
        if row.category != category:
            conn.execute(update(tag_table).where(tag_table.c.id == tag_id).values(category=category))
    else:
        result = conn.execute(tag_table.insert().values(name=name, category=category, post_count=0))
        tag_id = int(result.inserted_primary_key[0])
    cache[key] = tag_id
    return tag_id


def import_file(database_url: str, jsonl_path: Path, task_id: int | None, batch_size: int, recount_tag_counts: bool) -> dict:
    engine = create_engine(sync_database_url(database_url), future=True)
    post_table = Base.metadata.tables["post"]
    tag_table = Base.metadata.tables["tag"]
    post_tag_table = Base.metadata.tables["post_tag"]
    post_update_columns = [
        "task_id",
        "md5",
        "file_url",
        "preview_url",
        "sample_url",
        "source",
        "uploader_id",
        "uploader_name",
        "tag_string",
        "tag_count",
        "file_ext",
        "file_size",
        "image_width",
        "image_height",
        "score",
        "fav_count",
        "rating",
        "sources",
        "has_children",
        "is_deleted",
        "is_flagged",
        "created_at",
    ]

    with engine.begin() as conn:
        Base.metadata.create_all(conn)

    imported = 0
    errors = 0
    cache: dict[tuple[str, str], int] = {}
    conn = engine.connect()
    trans = conn.begin()
    try:
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    post = json.loads(line)
                    post_id = int(post["id"])
                    conn.execute(post_upsert(post_table, build_post_values(post, task_id), post_update_columns))
                    conn.execute(delete(post_tag_table).where(post_tag_table.c.post_id == post_id))
                    for category, tags in grouped_tags(post).items():
                        for name in dict.fromkeys(tags):
                            tag_id = resolve_tag_id(conn, tag_table, cache, name, category)
                            conn.execute(insert_ignore(post_tag_table, {"post_id": post_id, "tag_id": tag_id}))
                    imported += 1
                    if imported % batch_size == 0:
                        trans.commit()
                        trans = conn.begin()
                        print(f"imported={imported}", flush=True)
                except Exception as exc:
                    errors += 1
                    if errors <= 5:
                        print(f"error: {exc}", file=sys.stderr, flush=True)
        trans.commit()
        trans = conn.begin()
        if recount_tag_counts:
            conn.execute(update(tag_table).values(post_count=0))
            rows = conn.execute(select(post_tag_table.c.tag_id, func.count()).group_by(post_tag_table.c.tag_id)).all()
            for tag_id, observed in rows:
                conn.execute(
                    update(tag_table)
                    .where(tag_table.c.id == int(tag_id))
                    .values(post_count=int(observed))
                )
        trans.commit()
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
        engine.dispose()
    return {"imported": imported, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_path", type=Path)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), required=os.environ.get("DATABASE_URL") is None)
    parser.add_argument("--task-id", type=int)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--recount-tag-counts", action="store_true")
    args = parser.parse_args()
    stats = import_file(args.database_url, args.jsonl_path, args.task_id, args.batch_size, args.recount_tag_counts)
    print(f"OK fast import {stats}")
    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
