"""按 manifest 补跑失败的元数据分片。

默认只补跑没有 JSONL 或 JSONL 行数为 0 的 shard，避免把已经成功的大月重抓一遍。
如果需要，也可以开启 --retry-nonzero-errors，把 errors > 0 的 shard 一并重跑。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from services.ingest import import_jsonl
from workers.crawler import PostCrawlerWorker

from sync_recent_posts import inspect_jsonl, write_manifest


def load_manifest(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def shard_needs_retry(record: dict[str, Any], retry_nonzero_errors: bool) -> bool:
    jsonl = record.get("jsonl") or {}
    worker = record.get("worker") or {}
    exists = bool(jsonl.get("exists"))
    lines = int(jsonl.get("lines") or 0)
    errors = int(worker.get("errors") or 0)
    if not exists or lines == 0:
        return True
    if retry_nonzero_errors and errors > 0:
        return True
    return False


def select_retry_shards(manifest: dict[str, Any], args: argparse.Namespace) -> list[dict[str, Any]]:
    shards = [record for record in manifest.get("shards") or [] if shard_needs_retry(record, args.retry_nonzero_errors)]
    if args.task_ids:
        wanted = {int(task_id) for task_id in args.task_ids}
        shards = [record for record in shards if int(record.get("task_id") or 0) in wanted]
    if args.max_shards:
        shards = shards[: args.max_shards]
    return shards


async def import_if_requested(args: argparse.Namespace, jsonl_path: Path, task_id: int) -> dict[str, Any] | None:
    if args.skip_import:
        return None
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        stats = await import_jsonl(session, jsonl_path, task_id)
        await session.commit()
    return stats


def maybe_clean_existing_files(record: dict[str, Any], output_root: Path) -> None:
    task_id = int(record["task_id"])
    metadata_dir = output_root / "metadata"
    for suffix in [f"task_{task_id}_posts.jsonl", f"task_{task_id}_posts.invalid.jsonl", f"task_{task_id}.checkpoint.json"]:
        path = metadata_dir / suffix
        if path.exists():
            path.unlink()


async def run(args: argparse.Namespace) -> int:
    manifest_path = Path(args.manifest or Path(args.output_root or get_settings().OUTPUT_ROOT) / "metadata" / "sync_recent_posts_manifest.json")
    manifest = load_manifest(manifest_path)
    output_root = Path(args.output_root or manifest.get("output_root") or get_settings().OUTPUT_ROOT)
    retry_shards = select_retry_shards(manifest, args)
    retry_manifest_path = Path(args.retry_manifest or output_root / "metadata" / "sync_recent_posts_retry_manifest.json")

    retry_manifest: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest": str(manifest_path),
        "retry_nonzero_errors": bool(args.retry_nonzero_errors),
        "skip_import": bool(args.skip_import),
        "output_root": str(output_root),
        "shards": [],
    }

    print(f"retry shards={len(retry_shards)} source_manifest={manifest_path}")
    for index, record in enumerate(retry_shards, 1):
        task_id = int(record["task_id"])
        tag_filter = str(record["tag_filter"])
        if args.clean_before_retry:
            maybe_clean_existing_files(record, output_root)

        worker = PostCrawlerWorker(
            task_id=task_id,
            output_dir=output_root / "metadata",
            tag_filter=tag_filter,
            limit=args.limit,
            resume=not args.clean_before_retry,
        )
        print(f"[{index}/{len(retry_shards)}] retry task_id={task_id} {tag_filter}", flush=True)
        worker.run()
        jsonl_stats = inspect_jsonl(Path(worker.result["output_file"]))
        ingest_stats = await import_if_requested(args, Path(worker.result["output_file"]), task_id)
        retry_record = {
            "task_id": task_id,
            "tag_filter": tag_filter,
            "source_name": record.get("name"),
            "source_jsonl": record.get("jsonl"),
            "source_worker": record.get("worker"),
            "worker": worker.result,
            "jsonl": jsonl_stats,
            "ingest": ingest_stats,
        }
        retry_manifest["shards"].append(retry_record)
        write_manifest(retry_manifest_path, retry_manifest)
        print(
            "OK retry "
            f"task_id={task_id} lines={jsonl_stats['lines']} unique={jsonl_stats['unique_ids']} "
            f"dup={jsonl_stats['duplicate_ids']} errors={worker.result.get('errors', 0)}",
            flush=True,
        )
        if args.delay_seconds and index < len(retry_shards):
            time.sleep(args.delay_seconds)

    if not args.skip_import:
        await close_db()

    total_lines = sum(int(item["jsonl"]["lines"]) for item in retry_manifest["shards"])
    total_errors = sum(int(item["worker"].get("errors", 0)) for item in retry_manifest["shards"])
    retry_manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    retry_manifest["summary"] = {
        "shards": len(retry_manifest["shards"]),
        "total_lines": total_lines,
        "total_worker_errors": total_errors,
    }
    write_manifest(retry_manifest_path, retry_manifest)
    print(f"OK retry manifest={retry_manifest_path}")
    print(f"summary={retry_manifest['summary']}")
    return 0 if total_errors == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", help="Source sync manifest; defaults to output/metadata/sync_recent_posts_manifest.json")
    parser.add_argument("--retry-manifest", help="Output retry manifest path")
    parser.add_argument("--task-ids", nargs="*", type=int, help="Optional explicit task ids to retry")
    parser.add_argument("--max-shards", type=int)
    parser.add_argument("--delay-seconds", type=int, default=5)
    parser.add_argument("--limit", type=int, help="Optional debug limit applied to each retry shard")
    parser.add_argument("--skip-import", action="store_true")
    parser.add_argument("--retry-nonzero-errors", action="store_true", help="Also retry shards that have lines > 0 but worker errors > 0")
    parser.add_argument("--clean-before-retry", action="store_true", help="Delete old JSONL/checkpoint/invalid files before replay")
    parser.add_argument("--output-root")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
