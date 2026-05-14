"""快速导入 Danbooru JSONL 元数据到 PostgreSQL。"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, delete, select, text

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


def chunks(values: list[Any], size: int):
    for index in range(0, len(values), size):
        yield values[index : index + size]


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


def post_upsert(table, values: dict[str, Any] | list[dict[str, Any]], update_columns: list[str]):
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(table).values(values)
    return stmt.on_conflict_do_update(
        index_elements=[table.c.id],
        set_={name: getattr(stmt.excluded, name) for name in update_columns},
    )


def tag_upsert(table, values: list[dict[str, Any]]):
    from sqlalchemy.dialects.postgresql import insert

    stmt = insert(table).values(values)
    return stmt.on_conflict_do_update(
        index_elements=[table.c.name],
        set_={"category": stmt.excluded.category},
    )


def post_tag_insert_ignore(table, values: list[dict[str, Any]]):
    from sqlalchemy.dialects.postgresql import insert

    return insert(table).values(values).on_conflict_do_nothing()


def flush_batch(
    conn,
    post_table,
    tag_table,
    post_tag_table,
    posts: list[dict[str, Any]],
    task_id: int | None,
    tag_id_cache: dict[str, int],
    statement_chunk_size: int,
) -> int:
    """Write one parsed JSONL batch using set-oriented SQL operations."""
    if not posts:
        return 0

    posts_by_id: dict[int, dict[str, Any]] = {}
    for post in posts:
        posts_by_id[int(post["id"])] = post
    posts = list(posts_by_id.values())

    post_values = [build_post_values(post, task_id) for post in posts]
    post_ids = [int(post["id"]) for post in posts]
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
    for post_chunk in chunks(post_values, statement_chunk_size):
        conn.execute(post_upsert(post_table, post_chunk, post_update_columns))

    tag_values_by_name: dict[str, dict[str, Any]] = {}
    post_tag_names: list[tuple[int, str]] = []
    for post in posts:
        post_id = int(post["id"])
        for category, tags in grouped_tags(post).items():
            for name in dict.fromkeys(tags):
                tag_values_by_name[name] = {"name": name, "category": category, "post_count": 0}
                post_tag_names.append((post_id, name))

    missing_tag_names = [name for name in tag_values_by_name if name not in tag_id_cache]
    for name_chunk in chunks(missing_tag_names, statement_chunk_size):
        rows = conn.execute(
            select(tag_table.c.id, tag_table.c.name).where(tag_table.c.name.in_(name_chunk))
        ).all()
        for row in rows:
            tag_id_cache[str(row.name)] = int(row.id)

    new_tag_values = [tag_values_by_name[name] for name in missing_tag_names if name not in tag_id_cache]
    for tag_chunk in chunks(new_tag_values, statement_chunk_size):
        if tag_chunk:
            conn.execute(tag_upsert(tag_table, tag_chunk))

    unresolved_tag_names = [name for name in missing_tag_names if name not in tag_id_cache]
    for name_chunk in chunks(unresolved_tag_names, statement_chunk_size):
        rows = conn.execute(
            select(tag_table.c.id, tag_table.c.name).where(tag_table.c.name.in_(name_chunk))
        ).all()
        for row in rows:
            tag_id_cache[str(row.name)] = int(row.id)

    for post_id_chunk in chunks(post_ids, statement_chunk_size):
        conn.execute(delete(post_tag_table).where(post_tag_table.c.post_id.in_(post_id_chunk)))
    post_tag_values = list(
        {
            (post_id, tag_id_cache[name]): {"post_id": post_id, "tag_id": tag_id_cache[name]}
            for post_id, name in post_tag_names
            if name in tag_id_cache
        }.values()
    )
    for post_tag_chunk in chunks(post_tag_values, statement_chunk_size):
        if post_tag_chunk:
            conn.execute(post_tag_insert_ignore(post_tag_table, post_tag_chunk))
    return len(posts)


def import_file(
    database_url: str,
    jsonl_path: Path,
    task_id: int | None,
    batch_size: int,
    recount_tag_counts: bool,
    statement_chunk_size: int = 1000,
) -> dict:
    engine = create_engine(sync_database_url(database_url), future=True)
    post_table = Base.metadata.tables["post"]
    task_table = Base.metadata.tables["task"]
    tag_table = Base.metadata.tables["tag"]
    post_tag_table = Base.metadata.tables["post_tag"]
    with engine.begin() as conn:
        Base.metadata.create_all(conn)

    imported = 0
    errors = 0
    batch: list[dict[str, Any]] = []
    tag_id_cache: dict[str, int] = {}
    conn = engine.connect()
    trans = conn.begin()
    try:
        effective_task_id = task_id
        if effective_task_id is not None:
            task_exists = conn.execute(
                select(task_table.c.id).where(task_table.c.id == effective_task_id)
            ).first()
            if task_exists is None:
                effective_task_id = None
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    post = json.loads(line)
                    int(post["id"])
                    batch.append(post)
                    if len(batch) >= batch_size:
                        imported += flush_batch(
                            conn,
                            post_table,
                            tag_table,
                            post_tag_table,
                            batch,
                            effective_task_id,
                            tag_id_cache,
                            statement_chunk_size,
                        )
                        batch.clear()
                        trans.commit()
                        trans = conn.begin()
                        print(f"imported={imported}", flush=True)
                except Exception as exc:
                    errors += 1
                    if errors <= 5:
                        print(f"error: {exc}", file=sys.stderr, flush=True)
        if batch:
            imported += flush_batch(
                conn,
                post_table,
                tag_table,
                post_tag_table,
                batch,
                effective_task_id,
                tag_id_cache,
                statement_chunk_size,
            )
            batch.clear()
        trans.commit()
        trans = conn.begin()
        if recount_tag_counts:
            conn.execute(text("update tag set post_count = 0"))
            conn.execute(
                text(
                    """
                    update tag
                    set post_count = observed.count
                    from (
                        select tag_id, count(*)::integer as count
                        from post_tag
                        group by tag_id
                    ) as observed
                    where tag.id = observed.tag_id
                    """
                )
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
    parser.add_argument("--statement-chunk-size", type=int, default=1000)
    parser.add_argument("--recount-tag-counts", action="store_true")
    args = parser.parse_args()
    stats = import_file(
        args.database_url,
        args.jsonl_path,
        args.task_id,
        args.batch_size,
        args.recount_tag_counts,
        args.statement_chunk_size,
    )
    print(f"OK fast import {stats}")
    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
