"""每周增量同步 Danbooru 元数据并刷新榜单。

默认只处理元数据和榜单，不下载图片。该脚本面向调度器运行：

    python scripts/weekly_update_rankings.py --resume

可用 --dry-run 预览窗口和步骤，或用 --start-date/--end-date 手动补跑窗口。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import select

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
for path in [str(ROOT), str(BACKEND)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from config import get_settings
from database import close_db, get_session_factory, init_db
from models import JobLog, RankingSnapshot, SyncJob
from services.aggregation import rebuild_character_monthly_aggregates


@dataclass
class StepResult:
    name: str
    status: str
    elapsed_seconds: float
    details: dict[str, Any]


def parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def run_key(base_key: str, start: date, end: date) -> str:
    return f"{base_key}:{start.isoformat()}:{end.isoformat()}"


def configure_database(database_url: str | None) -> None:
    if database_url:
        os.environ["DATABASE_URL"] = database_url
        get_settings.cache_clear()


def run_command(command: list[str], env: dict[str, str] | None = None) -> dict[str, Any]:
    started = time.perf_counter()
    proc = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    elapsed = time.perf_counter() - started
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(command)}\n"
            f"stdout:\n{proc.stdout[-4000:]}\n"
            f"stderr:\n{proc.stderr[-4000:]}"
        )
    return {
        "command": command,
        "elapsed_seconds": round(elapsed, 3),
        "stdout_tail": proc.stdout[-4000:],
        "stderr_tail": proc.stderr[-4000:],
    }


async def latest_successful_window_end(job_key_prefix: str) -> date | None:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        row = await session.execute(
            select(SyncJob)
            .where(SyncJob.job_key.like(f"{job_key_prefix}:%"), SyncJob.status == "completed")
            .order_by(SyncJob.completed_at.desc())
        )
        job = row.scalars().first()
        if job and isinstance(job.params, dict):
            end = job.params.get("window_end")
            if end:
                return parse_date(str(end))
    return None


async def plan_window(args: argparse.Namespace) -> tuple[date, date]:
    today = datetime.now(UTC).date()
    end = parse_date(args.end_date) or today
    start = parse_date(args.start_date)
    if start is None:
        previous_end = await latest_successful_window_end(args.job_key)
        start = (previous_end + timedelta(days=1)) if previous_end else (end - timedelta(days=args.lookback_days))
    if start > end:
        raise ValueError(f"start_date must be <= end_date: {start} > {end}")
    return start, end


async def ensure_job(job_key: str, args: argparse.Namespace, start: date, end: date) -> int:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        row = await session.execute(select(SyncJob).where(SyncJob.job_key == job_key))
        job = row.scalar_one_or_none()
        if job is None:
            job = SyncJob(job_key=job_key, job_type="weekly_ranking")
            session.add(job)
            await session.flush()
        job.status = "running"
        job.started_at = datetime.now(UTC)
        job.completed_at = None
        job.params = {
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "output_root": args.output_root,
            "recent_months": args.recent_months,
            "top_n": args.top_n,
            "min_count": args.min_count,
        }
        session.add(
            JobLog(
                job_id=job.id,
                level="INFO",
                message="weekly ranking update started",
                context=job.params,
            )
        )
        await session.commit()
        return int(job.id)


async def finish_job(job_id: int, status: str, context: dict[str, Any]) -> None:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        job = await session.get(SyncJob, job_id)
        if job is None:
            return
        job.status = status
        job.completed_at = datetime.now(UTC)
        params = dict(job.params or {})
        params.update(context)
        job.params = params
        session.add(
            JobLog(
                job_id=job.id,
                level="INFO" if status == "completed" else "ERROR",
                message=f"weekly ranking update {status}",
                context=context,
            )
        )
        await session.commit()


async def snapshot_summary() -> dict[str, Any]:
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        rows = await session.execute(
            select(RankingSnapshot)
            .where(RankingSnapshot.ranking_type.in_(["recent", "emerging"]))
            .order_by(RankingSnapshot.ranking_type, RankingSnapshot.generated_at.desc())
        )
        latest: dict[str, dict[str, Any]] = {}
        for snapshot in rows.scalars().all():
            if snapshot.ranking_type in latest:
                continue
            latest[snapshot.ranking_type] = {
                "id": snapshot.id,
                "generated_at": snapshot.generated_at.isoformat() if snapshot.generated_at else None,
                "json": snapshot.export_json_path,
                "csv": snapshot.export_csv_path,
            }
    return latest


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def latest_export_file(export_dir: Path, pattern: str) -> Path:
    matches = sorted(export_dir.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"no export file matches {export_dir / pattern}")
    return matches[0]


async def run_incremental_aggregate(args: argparse.Namespace, start: date, end: date) -> dict[str, Any]:
    await close_db()
    await init_db()
    factory = await get_session_factory()
    async with factory() as session:
        stats = await rebuild_character_monthly_aggregates(session, start, end)
        await session.commit()
    await close_db()
    return stats


async def run(args: argparse.Namespace) -> int:
    configure_database(args.database_url)
    start, end = await plan_window(args)
    output_root = Path(args.output_root or get_settings().OUTPUT_ROOT)
    if args.existing_manifest:
        manifest_path = args.existing_manifest
        manifest_name = manifest_path.name
    else:
        manifest_name = args.manifest_name or f"weekly_update_{start.isoformat()}_{end.isoformat()}_manifest.json"
        manifest_path = output_root / "metadata" / manifest_name
    python = sys.executable

    actual_job_key = run_key(args.job_key, start, end)
    plan = {
        "job_key": actual_job_key,
        "job_key_prefix": args.job_key,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "output_root": str(output_root),
        "manifest": str(manifest_path),
        "existing_manifest": bool(args.existing_manifest),
        "steps": [
            "sync metadata JSONL",
            "import JSONL shards",
            "sync manifest to job state",
            "refresh monthly aggregates for touched months",
            "build recent ranking",
            "build emerging ranking",
            "validate exports",
        ],
        "sync_limit": args.sync_limit,
    }
    if args.dry_run:
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        await close_db()
        return 0

    job_id = await ensure_job(actual_job_key, args, start, end)
    step_results: list[StepResult] = []
    env = os.environ.copy()
    if args.database_url:
        env["DATABASE_URL"] = args.database_url

    try:
        if args.existing_manifest:
            step_results.append(
                StepResult(
                    "sync",
                    "skipped",
                    0.0,
                    {"reason": "using existing manifest", "manifest": str(manifest_path)},
                )
            )
        else:
            sync_cmd = [
                python,
                "scripts/sync_recent_posts.py",
                "--start-date",
                start.isoformat(),
                "--end-date",
                end.isoformat(),
                "--shard",
                args.shard,
                "--skip-import",
                "--output-root",
                str(output_root),
                "--manifest-name",
                manifest_name,
                "--task-id-prefix",
                str(args.task_id_prefix),
            ]
            if args.sync_limit is not None:
                sync_cmd.extend(["--limit", str(args.sync_limit)])
            if args.resume:
                sync_cmd.append("--resume")
            details = run_command(sync_cmd, env)
            step_results.append(StepResult("sync", "completed", details["elapsed_seconds"], details))

        manifest = load_manifest(manifest_path)
        import_details: list[dict[str, Any]] = []
        for shard in manifest.get("shards") or []:
            jsonl_path = Path(shard.get("jsonl", {}).get("path") or "")
            if not jsonl_path.exists():
                continue
            import_cmd = [
                python,
                "scripts/import_jsonl_fast.py",
                str(jsonl_path),
                "--batch-size",
                str(args.batch_size),
                "--statement-chunk-size",
                str(args.statement_chunk_size),
                "--recount-tag-counts",
            ]
            if args.database_url:
                import_cmd.extend(["--database-url", args.database_url])
            if shard.get("task_id") is not None:
                import_cmd.extend(["--task-id", str(shard["task_id"])])
            import_details.append(run_command(import_cmd, env))
        step_results.append(
            StepResult(
                "import",
                "completed",
                round(sum(item["elapsed_seconds"] for item in import_details), 3),
                {"shards": import_details},
            )
        )

        manifest_sync_cmd = [
            python,
            "scripts/sync_manifest_to_db.py",
            str(manifest_path),
            "--job-key",
            actual_job_key,
        ]
        if args.database_url:
            manifest_sync_cmd.extend(["--database-url", args.database_url])
        details = run_command(manifest_sync_cmd, env)
        step_results.append(StepResult("manifest", "completed", details["elapsed_seconds"], details))

        started = time.perf_counter()
        aggregate_stats = await run_incremental_aggregate(args, start, end)
        step_results.append(
            StepResult(
                "aggregate",
                "completed",
                round(time.perf_counter() - started, 3),
                aggregate_stats,
            )
        )

        recent_cmd = [
            python,
            "scripts/build_character_list.py",
            "--recent-months",
            str(args.recent_months),
            "--top-n",
            str(args.top_n),
            "--min-count",
            str(args.min_count),
            "--output-root",
            str(output_root),
        ]
        if args.database_url:
            recent_cmd.extend(["--database-url", args.database_url])
        details = run_command(recent_cmd, env)
        step_results.append(StepResult("recent_ranking", "completed", details["elapsed_seconds"], details))

        emerging_cmd = [
            python,
            "scripts/build_emerging_character_list.py",
            "--top-n",
            str(args.top_n),
            "--min-count",
            str(args.min_count),
            "--min-recent-count",
            str(args.min_recent_count),
            "--max-age-days",
            str(args.max_age_days),
            "--output-root",
            str(output_root),
        ]
        if args.database_url:
            emerging_cmd.extend(["--database-url", args.database_url])
        details = run_command(emerging_cmd, env)
        step_results.append(StepResult("emerging_ranking", "completed", details["elapsed_seconds"], details))

        export_dir = output_root / "exports"
        recent_json = export_dir / f"character_list_recent_{args.recent_months}m_top_{args.top_n}.json"
        recent_csv = export_dir / f"character_list_recent_{args.recent_months}m_top_{args.top_n}.csv"
        emerging_json = latest_export_file(export_dir, "character_list_emerging_*_top_*.json")
        emerging_csv = latest_export_file(export_dir, "character_list_emerging_*_top_*.csv")
        validate_details = [
            run_command([python, "scripts/validate_character_export.py", str(recent_json), str(recent_csv)], env),
            run_command(
                [
                    python,
                    "scripts/validate_emerging_export.py",
                    str(emerging_json),
                    str(emerging_csv),
                    "--max-age-days",
                    str(args.max_age_days),
                ],
                env,
            ),
        ]
        step_results.append(
            StepResult(
                "validate",
                "completed",
                round(sum(item["elapsed_seconds"] for item in validate_details), 3),
                {"commands": validate_details},
            )
        )

        snapshots = await snapshot_summary()
        context = {
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "manifest": str(manifest_path),
            "steps": [result.__dict__ for result in step_results],
            "snapshots": snapshots,
        }
        await finish_job(job_id, "completed", context)
        print(json.dumps(context, ensure_ascii=False, indent=2, default=str))
        return 0
    except Exception as exc:
        context = {
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "manifest": str(manifest_path),
            "steps": [result.__dict__ for result in step_results],
            "error": str(exc),
        }
        await finish_job(job_id, "failed", context)
        print(json.dumps(context, ensure_ascii=False, indent=2, default=str), file=sys.stderr)
        return 1
    finally:
        await close_db()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", help="SQLAlchemy async database URL")
    parser.add_argument("--output-root", default=None)
    parser.add_argument("--job-key", default="weekly-ranking-update")
    parser.add_argument("--start-date", help="YYYY-MM-DD")
    parser.add_argument("--end-date", help="YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--recent-months", type=int, default=6)
    parser.add_argument("--top-n", type=int, default=200)
    parser.add_argument("--min-count", type=int, default=50)
    parser.add_argument("--min-recent-count", type=int, default=10)
    parser.add_argument("--max-age-days", type=int, default=1095)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--statement-chunk-size", type=int, default=1000)
    parser.add_argument("--shard", choices=["none", "monthly", "quarterly"], default="none")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest-name")
    parser.add_argument("--existing-manifest", type=Path, help="Use an existing sync manifest and skip network sync")
    parser.add_argument("--task-id-prefix", type=int, default=930000)
    parser.add_argument("--sync-limit", type=int, help="Debug/validation limit passed to sync_recent_posts.py")
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
