"""快速导入 Danbooru JSONL 元数据到 SQLite。

用于大批量 recent posts 同步后的离线导入，避免 ORM 逐行导入过慢。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


TAG_FIELDS = {
    "general": "tag_string_general",
    "character": "tag_string_character",
    "copyright": "tag_string_copyright",
    "artist": "tag_string_artist",
    "meta": "tag_string_meta",
}


def split_tags(value):
    return [t.strip() for t in (value or "").split() if t.strip()]


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None).isoformat()
    except Exception:
        return None


def grouped_tags(post):
    return {category: split_tags(post.get(field)) for category, field in TAG_FIELDS.items()}


def ensure_schema(conn: sqlite3.Connection):
    # The app creates the full schema. This script only adds missing compatibility columns.
    cols = {row[1] for row in conn.execute("pragma table_info(post)").fetchall()}
    for name, ddl in {
        "file_url": "TEXT",
        "preview_url": "TEXT",
        "sample_url": "TEXT",
        "source": "TEXT",
        "fav_count": "INTEGER DEFAULT 0",
    }.items():
        if name not in cols:
            conn.execute(f"alter table post add column {name} {ddl}")


def tag_id(conn: sqlite3.Connection, cache: dict[tuple[str, str], int], name: str, category: str) -> int:
    key = (name, category)
    if key in cache:
        return cache[key]
    row = conn.execute("select id, category from tag where name=?", (name,)).fetchone()
    if row:
        tid = int(row[0])
        if row[1] != category:
            conn.execute("update tag set category=?, updated_at=current_timestamp where id=?", (category, tid))
    else:
        cur = conn.execute(
            "insert into tag (name, category, post_count, updated_at) values (?, ?, 0, current_timestamp)",
            (name, category),
        )
        tid = int(cur.lastrowid)
    cache[key] = tid
    return tid


def import_file(db_path: Path, jsonl_path: Path, task_id: int | None, batch_size: int, recount_tag_counts: bool) -> dict:
    conn = sqlite3.connect(db_path)
    conn.execute("pragma journal_mode=wal")
    conn.execute("pragma synchronous=normal")
    ensure_schema(conn)
    imported = 0
    errors = 0
    cache: dict[tuple[str, str], int] = {}
    touched_tags: set[int] = set()
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                post = json.loads(line)
                post_id = int(post["id"])
                conn.execute(
                    """
                    insert into post (
                        id, task_id, md5, file_url, preview_url, sample_url, source,
                        uploader_id, uploader_name, tag_string, tag_count, file_ext, file_size,
                        image_width, image_height, score, fav_count, rating, sources,
                        has_children, is_deleted, is_flagged, created_at, fetched_at, file_verified
                    )
                    values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, 0)
                    on conflict(id) do update set
                        task_id=excluded.task_id,
                        md5=excluded.md5,
                        file_url=excluded.file_url,
                        preview_url=excluded.preview_url,
                        sample_url=excluded.sample_url,
                        source=excluded.source,
                        uploader_id=excluded.uploader_id,
                        uploader_name=excluded.uploader_name,
                        tag_string=excluded.tag_string,
                        tag_count=excluded.tag_count,
                        file_ext=excluded.file_ext,
                        file_size=excluded.file_size,
                        image_width=excluded.image_width,
                        image_height=excluded.image_height,
                        score=excluded.score,
                        fav_count=excluded.fav_count,
                        rating=excluded.rating,
                        sources=excluded.sources,
                        has_children=excluded.has_children,
                        is_deleted=excluded.is_deleted,
                        is_flagged=excluded.is_flagged,
                        created_at=excluded.created_at
                    """,
                    (
                        post_id,
                        task_id,
                        post.get("md5"),
                        post.get("file_url"),
                        post.get("preview_url"),
                        post.get("sample_url"),
                        post.get("source"),
                        post.get("uploader_id"),
                        post.get("uploader_name"),
                        post.get("tag_string"),
                        int(post.get("tag_count") or 0),
                        post.get("file_ext"),
                        post.get("file_size"),
                        post.get("image_width"),
                        post.get("image_height"),
                        int(post.get("score") or 0),
                        int(post.get("fav_count") or 0),
                        post.get("rating"),
                        json.dumps(post.get("sources") or [], ensure_ascii=False),
                        int(bool(post.get("has_children", False))),
                        int(bool(post.get("is_deleted", False))),
                        int(bool(post.get("is_flagged", False))),
                        parse_datetime(post.get("created_at")),
                    ),
                )
                conn.execute("delete from post_tag where post_id=?", (post_id,))
                for category, tags in grouped_tags(post).items():
                    for name in dict.fromkeys(tags):
                        tid = tag_id(conn, cache, name, category)
                        touched_tags.add(tid)
                        conn.execute(
                            "insert or ignore into post_tag (post_id, tag_id) values (?, ?)",
                            (post_id, tid),
                        )
                imported += 1
                if imported % batch_size == 0:
                    conn.commit()
                    print(f"imported={imported}", flush=True)
            except Exception as exc:
                errors += 1
                if errors <= 5:
                    print(f"error: {exc}", file=sys.stderr, flush=True)
    conn.commit()
    if recount_tag_counts:
        for tid in touched_tags:
            observed = conn.execute("select count(*) from post_tag where tag_id=?", (tid,)).fetchone()[0]
            row = conn.execute("select post_count from tag where id=?", (tid,)).fetchone()
            if row and int(row[0] or 0) < observed:
                conn.execute("update tag set post_count=?, updated_at=current_timestamp where id=?", (observed, tid))
        conn.commit()
    conn.close()
    return {"imported": imported, "errors": errors}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("jsonl_path", type=Path)
    parser.add_argument("--db", type=Path, default=Path("muse_dataload.db"))
    parser.add_argument("--task-id", type=int)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--recount-tag-counts", action="store_true")
    args = parser.parse_args()
    stats = import_file(args.db, args.jsonl_path, args.task_id, args.batch_size, args.recount_tag_counts)
    print(f"OK fast import {stats}")
    return 0 if stats["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
