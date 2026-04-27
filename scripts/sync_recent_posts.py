"""同步最近时间窗口的 Danbooru 帖子元数据。

默认只同步元数据，不下载图片。长窗口建议使用分片和 resume：

    python scripts/sync_recent_posts.py --recent-months 24 --shard monthly --resume --skip-import

分片 JSONL 会写入 output/metadata/，manifest 会记录每片状态、行数、重复 ID
和错误数。需要把 JSONL 导入 PostgreSQL 时，再去掉 --skip-import 或使用
scripts/import_jsonl_fast.py 离线导入。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
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


@dataclass
class Shard:
    index: int
    name: str
    start_date: date
    end_date: date
    task_id: int

    @property
    def tag_filter(self) -> str:
        return f"date:{self.start_date.isoformat()}..{self.end_date.isoformat()}"


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def monthly_ranges(start: date, end: date) -> list[tuple[date, date]]:
    ranges: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        shard_end = min(month_end(cursor), end)
        ranges.append((cursor, shard_end))
        cursor = shard_end + timedelta(days=1)
    return ranges


def grouped_ranges(ranges: list[tuple[date, date]], group_size: int) -> list[tuple[date, date]]:
    grouped: list[tuple[date, date]] = []
    for i in range(0, len(ranges), group_size):
        chunk = ranges[i : i + group_size]
        grouped.append((chunk[0][0], chunk[-1][1]))
    return grouped


def build_shards(args: argparse.Namespace) -> list[Shard]:
    today = datetime.now(timezone.utc).date()
    start = parse_date(args.start_date) or (today - timedelta(days=args.recent_months * 30))
    end = parse_date(args.end_date) or today
    if start > end:
        raise ValueError(f"start_date must be <= end_date: {start} > {end}")

    if args.tag_filter:
        return [Shard(index=1, name="custom", start_date=start, end_date=end, task_id=args.task_id)]

    if args.shard == "none":
        ranges = [(start, end)]
    else:
        ranges = monthly_ranges(start, end)
        if args.shard == "quarterly":
            ranges = grouped_ranges(ranges, 3)

    base_task_id = args.task_id_prefix or args.task_id
    shards: list[Shard] = []
    for index, (shard_start, shard_end) in enumerate(ranges, 1):
        if args.shard == "none":
            name = f"{shard_start.isoformat()}_{shard_end.isoformat()}"
            task_id = args.task_id
        else:
            name = f"{args.shard}_{shard_start.isoformat()}_{shard_end.isoformat()}"
            task_id = base_task_id + index
        shards.append(Shard(index=index, name=name, start_date=shard_start, end_date=shard_end, task_id=task_id))
    return shards


def inspect_jsonl(path: Path) -> dict[str, Any]:
    stats: dict[str, Any] = {
        "path": str(path),
        "exists": path.exists(),
        "bytes": path.stat().st_size if path.exists() else 0,
        "lines": 0,
        "unique_ids": 0,
        "duplicate_ids": 0,
        "invalid_lines": 0,
    }
    if not path.exists():
        return stats

    seen: set[int] = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            stats["lines"] += 1
            try:
                post = json.loads(line)
                post_id = int(post["id"])
            except Exception:
                stats["invalid_lines"] += 1
                continue
            if post_id in seen:
                stats["duplicate_ids"] += 1
            seen.add(post_id)
    stats["unique_ids"] = len(seen)
    return stats


def write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


async def import_if_requested(args: argparse.Namespace, jsonl_path: Path, task_id: int) -> dict[str, Any] | None:
    if args.skip_import:
        return None
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        stats = await import_jsonl(session, jsonl_path, task_id)
        await session.commit()
    return stats


async def run(args: argparse.Namespace) -> int:
    output_root = Path(args.output_root or get_settings().OUTPUT_ROOT)
    metadata_dir = output_root / "metadata"
    manifest_path = metadata_dir / args.manifest_name
    shards = build_shards(args)

    manifest: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "metadata_jsonl_only" if args.skip_import else "metadata_jsonl_and_import",
        "recent_months": args.recent_months,
        "shard": args.shard,
        "resume": bool(args.resume),
        "limit_per_shard": args.limit,
        "output_root": str(output_root),
        "shards": [],
    }

    print(f"计划同步 shards={len(shards)} output={metadata_dir} skip_import={args.skip_import}")
    for shard in shards:
        tag_filter = args.tag_filter or shard.tag_filter
        worker = PostCrawlerWorker(
            task_id=shard.task_id,
            output_dir=metadata_dir,
            tag_filter=tag_filter,
            tags=args.tags or [],
            limit=args.limit,
            resume=args.resume,
        )
        print(f"[{shard.index}/{len(shards)}] start task_id={shard.task_id} {tag_filter}", flush=True)
        worker.run()
        jsonl_stats = inspect_jsonl(Path(worker.result["output_file"]))
        ingest_stats = await import_if_requested(args, Path(worker.result["output_file"]), shard.task_id)
        shard_record = {
            **asdict(shard),
            "start_date": shard.start_date.isoformat(),
            "end_date": shard.end_date.isoformat(),
            "tag_filter": tag_filter,
            "worker": worker.result,
            "jsonl": jsonl_stats,
            "ingest": ingest_stats,
        }
        manifest["shards"].append(shard_record)
        write_manifest(manifest_path, manifest)
        print(
            "OK shard "
            f"task_id={shard.task_id} lines={jsonl_stats['lines']} "
            f"unique={jsonl_stats['unique_ids']} dup={jsonl_stats['duplicate_ids']} "
            f"errors={worker.result.get('errors', 0)}",
            flush=True,
        )

    if not args.skip_import:
        await close_db()

    total_lines = sum(int(item["jsonl"]["lines"]) for item in manifest["shards"])
    total_unique = sum(int(item["jsonl"]["unique_ids"]) for item in manifest["shards"])
    total_errors = sum(int(item["worker"].get("errors", 0)) for item in manifest["shards"])
    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest["summary"] = {
        "shards": len(shards),
        "total_lines": total_lines,
        "total_unique_ids_by_shard": total_unique,
        "total_worker_errors": total_errors,
    }
    write_manifest(manifest_path, manifest)
    print(f"OK sync recent manifest={manifest_path}")
    print(f"summary={manifest['summary']}")
    return 0 if total_errors == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--start-date", help="Override window start date, YYYY-MM-DD")
    parser.add_argument("--end-date", help="Override window end date, YYYY-MM-DD")
    parser.add_argument("--shard", choices=["none", "monthly", "quarterly"], default="none")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--skip-import", action="store_true", help="Keep JSONL only; do not import into PostgreSQL")
    parser.add_argument("--tag-filter")
    parser.add_argument("--tags", nargs="*")
    parser.add_argument("--limit", type=int, help="Debug limit applied to each shard")
    parser.add_argument("--task-id", type=int, default=900001)
    parser.add_argument("--task-id-prefix", type=int, help="Base task id for sharded runs; shard index is added")
    parser.add_argument("--output-root")
    parser.add_argument("--manifest-name", default="sync_recent_posts_manifest.json")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
