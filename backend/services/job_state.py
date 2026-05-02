"""同步 manifest 到正式任务状态表。"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import JobLog, SyncJob, SyncShard


def shard_status(record: dict[str, Any]) -> str:
    jsonl = record.get("jsonl") or {}
    if jsonl.get("exists") and int(jsonl.get("lines") or 0) > 0:
        return "completed"
    return "failed"


async def sync_manifest_to_db(
    session: AsyncSession,
    manifest: dict[str, Any],
    job_key: str,
    manifest_path: Path | None = None,
) -> dict[str, int]:
    """把 sync_recent_posts manifest 固化到 sync_job/sync_shard。"""
    shards = list(manifest.get("shards") or [])
    job_row = await session.execute(select(SyncJob).where(SyncJob.job_key == job_key))
    job = job_row.scalar_one_or_none()
    if job is None:
        job = SyncJob(job_key=job_key, job_type="metadata")
        session.add(job)
        await session.flush()

    completed = 0
    failed = 0
    for record in shards:
        key = str(record.get("shard_key") or record.get("tag_filter") or record.get("task_id"))
        status = shard_status(record)
        completed += 1 if status == "completed" else 0
        failed += 1 if status == "failed" else 0

        shard_row = await session.execute(
            select(SyncShard).where(SyncShard.job_id == job.id, SyncShard.shard_key == key)
        )
        shard = shard_row.scalar_one_or_none()
        if shard is None:
            shard = SyncShard(job_id=job.id, shard_key=key)
            session.add(shard)
        jsonl = record.get("jsonl") or {}
        worker = record.get("worker") or {}
        shard.task_id = record.get("task_id")
        shard.tag_filter = record.get("tag_filter")
        shard.output_path = jsonl.get("path")
        shard.status = status
        shard.line_count = int(jsonl.get("lines") or 0)
        shard.duplicate_count = int(jsonl.get("duplicate_ids") or 0)
        shard.invalid_count = int(jsonl.get("invalid_lines") or 0)
        shard.error_count = int(worker.get("errors") or 0)
        shard.checkpoint = record.get("checkpoint") or {}
        shard.completed_at = datetime.now(UTC) if status == "completed" else None

    job.params = {
        "manifest_path": str(manifest_path) if manifest_path else None,
        "source": "sync_recent_posts_manifest",
    }
    job.total_shards = len(shards)
    job.completed_shards = completed
    job.failed_shards = failed
    job.status = "completed" if shards and failed == 0 else "failed"
    job.completed_at = datetime.now(UTC) if job.status == "completed" else None
    session.add(
        JobLog(
            job_id=job.id,
            level="INFO" if failed == 0 else "WARN",
            message="manifest synchronized",
            context={
                "total_shards": len(shards),
                "completed_shards": completed,
                "failed_shards": failed,
            },
        )
    )
    await session.flush()
    return {
        "jobs": 1,
        "shards": len(shards),
        "completed_shards": completed,
        "failed_shards": failed,
    }
