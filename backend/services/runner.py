"""任务执行编排。"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import select

from config import get_settings
from database import get_session_factory
from models import Task, TaskLog
from services.danbooru import get_danbooru_client
from services.ingest import (
    import_danbooru_aliases,
    import_danbooru_implications,
    import_danbooru_tags,
    import_jsonl,
)
from services.ranking import build_character_ranking
from workers.crawler import PostCrawlerWorker


async def _log(session, task_id: int, level: str, message: str) -> None:
    session.add(TaskLog(task_id=task_id, level=level, message=message))
    await session.flush()


async def _load_task(session, task_id: int) -> Task:
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if task is None:
        raise RuntimeError(f"任务不存在: {task_id}")
    return task


async def _mark_task(task: Task, status: str, **fields: Any) -> None:
    task.status = status
    task.updated_at = datetime.utcnow()
    if status == "running" and not task.started_at:
        task.started_at = datetime.utcnow()
    if status in {"completed", "failed", "cancelled"}:
        task.completed_at = datetime.utcnow()
    for key, value in fields.items():
        setattr(task, key, value)


def _fetch_tags_sync(params: Dict[str, Any]) -> List[Any]:
    client = get_danbooru_client()
    category = params.get("category") or "character"
    min_post_count = int(params.get("min_post_count") or 0)
    limit = int(params.get("limit") or params.get("top_n") or 1000)
    imported: List[Any] = []
    page = 1
    while len(imported) < limit:
        batch = client.get_tags(
            limit=min(1000, limit - len(imported)),
            page=page,
            category=category,
            order="count",
        )
        if not batch:
            break
        for tag in batch:
            if tag.post_count >= min_post_count:
                imported.append(tag)
            if len(imported) >= limit:
                break
        page += 1
    return imported


def _fetch_aliases_sync(params: Dict[str, Any]) -> List[Any]:
    client = get_danbooru_client()
    limit = int(params.get("alias_limit") or params.get("limit") or 1000)
    imported: List[Any] = []
    page = 1
    while len(imported) < limit:
        batch = client.get_tag_aliases(limit=min(1000, limit - len(imported)), page=page)
        if not batch:
            break
        imported.extend(batch)
        page += 1
    return imported[:limit]


def _fetch_implications_sync(params: Dict[str, Any]) -> List[Any]:
    client = get_danbooru_client()
    limit = int(params.get("implication_limit") or params.get("limit") or 1000)
    imported: List[Any] = []
    page = 1
    while len(imported) < limit:
        batch = client.get_tag_implications(limit=min(1000, limit - len(imported)), page=page)
        if not batch:
            break
        imported.extend(batch)
        page += 1
    return imported[:limit]


async def _run_posts_task(task: Task, params: Dict[str, Any], output_root: Path) -> Dict[str, Any]:
    metadata_dir = output_root / "metadata"
    worker = PostCrawlerWorker(
        task_id=task.id,
        output_dir=metadata_dir,
        start_id=params.get("start_id"),
        end_id=params.get("end_id"),
        tags=params.get("tags") or [],
        tag_filter=params.get("tag_filter"),
        danbooru_ids=params.get("danbooru_ids") or [],
        limit=params.get("limit"),
        resume=bool(params.get("resume", False)),
    )
    await asyncio.to_thread(worker.run)
    return worker.result


async def _import_all_metadata(session, output_root: Path, task_id: int) -> Dict[str, int]:
    metadata_dir = output_root / "metadata"
    totals = {"imported": 0, "created": 0, "errors": 0}
    for path in sorted(metadata_dir.glob("task_*_posts.jsonl")):
        stats = await import_jsonl(session, path, task_id if f"task_{task_id}_" in path.name else None)
        for key in totals:
            totals[key] += stats.get(key, 0)
    return totals


async def run_task_background(task_id: int) -> None:
    """后台执行任务，负责状态、日志和数据库入库。"""
    settings = get_settings()
    output_root = Path(settings.OUTPUT_ROOT)
    output_root.mkdir(parents=True, exist_ok=True)

    factory = await get_session_factory()
    async with factory() as session:
        try:
            task = await _load_task(session, task_id)
            params = dict(task.params or {})
            await _mark_task(task, "running", progress=0.0)
            await _log(session, task_id, "INFO", f"任务开始: {task.task_type}")
            await session.commit()

            if task.task_type == "posts":
                result = await _run_posts_task(task, params, output_root)
                await _log(session, task_id, "INFO", f"元数据抓取完成: {result}")
                ingest_stats = await import_jsonl(session, Path(result["output_file"]), task_id)
                total = max(ingest_stats["imported"], result.get("total_seen", 0))
                await _mark_task(
                    task,
                    "completed" if result.get("status") == "completed" else "cancelled",
                    progress=1.0,
                    processed_count=ingest_stats["imported"],
                    total_count=total,
                    error_count=result.get("errors", 0) + ingest_stats["errors"],
                )
                await _log(session, task_id, "INFO", f"入库完成: {ingest_stats}")

            elif task.task_type == "tags":
                tags = await asyncio.to_thread(_fetch_tags_sync, params)
                stats = await import_danbooru_tags(session, tags)
                alias_count = 0
                implication_count = 0
                if params.get("include_aliases", True):
                    aliases = await asyncio.to_thread(_fetch_aliases_sync, params)
                    alias_stats = await import_danbooru_aliases(session, aliases)
                    alias_count = alias_stats["imported"]
                if params.get("include_implications", False):
                    implications = await asyncio.to_thread(_fetch_implications_sync, params)
                    implication_stats = await import_danbooru_implications(session, implications)
                    implication_count = implication_stats["imported"]
                await _mark_task(
                    task,
                    "completed",
                    progress=1.0,
                    processed_count=stats["imported"],
                    total_count=stats["imported"],
                )
                await _log(
                    session,
                    task_id,
                    "INFO",
                    f"标签入库完成: tags={stats['imported']} aliases={alias_count} implications={implication_count}",
                )

            elif task.task_type == "characters":
                await _import_all_metadata(session, output_root, task_id)
                result = await build_character_ranking(
                    session=session,
                    output_root=output_root,
                    min_post_count=int(params.get("min_post_count") or 50),
                    recent_months=int(params.get("recent_months") or 6),
                    top_n=int(params.get("top_n") or 200),
                )
                await _mark_task(
                    task,
                    "completed",
                    progress=1.0,
                    processed_count=result["total_count"],
                    total_count=result["total_count"],
                )
                await _log(
                    session,
                    task_id,
                    "INFO",
                    f"角色榜单完成: {result['total_count']} 条，JSON={result['json_path']}",
                )
            else:
                raise RuntimeError(f"不支持的任务类型: {task.task_type}")

            await session.commit()
        except Exception as exc:
            await session.rollback()
            async with factory() as error_session:
                task = await _load_task(error_session, task_id)
                await _mark_task(task, "failed", error_count=(task.error_count or 0) + 1)
                await _log(error_session, task_id, "ERROR", str(exc))
                await error_session.commit()
