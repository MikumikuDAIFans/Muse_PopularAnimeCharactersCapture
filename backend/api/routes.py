"""API 路由定义"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List
import json

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from sqlalchemy import select, func, delete, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from database import get_db_session
from models import Task, Project, Post, Tag, Character, Copyright, CharacterCopyright, TaskLog, SystemStats
from schemas import (
    ProjectCreate, ProjectUpdate, ProjectResponse,
    TaskCreate, TaskUpdate, TaskResponse, TaskDetailResponse, TaskLogResponse,
    PostResponse, PostListResponse,
    TagResponse, TagListResponse,
    CharacterResponse, CharacterListResponse, CharacterExportResponse,
    DatasetExportRequest, DatasetExportResponse,
    DashboardStats, TaskStats, HealthResponse, MessageResponse,
)
from services.runner import run_task_background
from services.dataset import export_character_dataset
from services.ranking import build_character_ranking
from services.emerging import build_emerging_character_ranking, refresh_emerging_payload_ages
from config import get_settings


router = APIRouter()


def _load_export_payload(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _parse_export_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return datetime.utcnow()


def _recent_export_path() -> Path:
    return Path(get_settings().OUTPUT_ROOT) / "exports" / "character_list_recent_6m_top_200.json"


def _recent_export_csv_path() -> Path:
    return Path(get_settings().OUTPUT_ROOT) / "exports" / "character_list_recent_6m_top_200.csv"


def _payload_matches_recent_filters(payload: dict, n: int, recent_months: int, min_count: int) -> bool:
    filters = payload.get("filters", {})
    return (
        int(filters.get("top_n", n)) >= n
        and int(filters.get("recent_months", recent_months)) == recent_months
        and int(filters.get("min_post_count", min_count)) == min_count
    )


# ========== 项目管理 ==========

@router.get("/projects", response_model=List[ProjectResponse], tags=["项目"])
async def list_projects(
    session: AsyncSession = Depends(get_db_session),
):
    """列出所有项目"""
    result = await session.execute(select(Project).order_by(Project.created_at.desc()))
    return result.scalars().all()


@router.post("/projects", response_model=ProjectResponse, tags=["项目"])
async def create_project(
    data: ProjectCreate,
    session: AsyncSession = Depends(get_db_session),
):
    """创建新项目"""
    project = Project(name=data.name, description=data.description)
    session.add(project)
    await session.flush()
    await session.refresh(project)
    return project


@router.get("/projects/{project_id}", response_model=ProjectResponse, tags=["项目"])
async def get_project(project_id: int, session: AsyncSession = Depends(get_db_session)):
    """获取项目详情"""
    result = await session.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="项目不存在")
    return project


@router.delete("/projects/{project_id}", response_model=MessageResponse, tags=["项目"])
async def delete_project(project_id: int, session: AsyncSession = Depends(get_db_session)):
    """删除项目"""
    result = await session.execute(select(Project).where(Project.id == project_id))
    project = result.scalar_one_or_none()
    if not project:
        raise HTTPException(status_code=404, detail="项目不存在")
    await session.delete(project)
    return MessageResponse(message="项目已删除")


# ========== 任务管理 ==========

@router.get("/tasks", response_model=List[TaskResponse], tags=["任务"])
async def list_tasks(
    status: Optional[str] = Query(None, description="状态筛选"),
    project_id: Optional[int] = Query(None, description="项目ID"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """列出任务"""
    query = select(Task)

    if status:
        query = query.where(Task.status == status)
    if project_id is not None:
        query = query.where(Task.project_id == project_id)

    query = query.order_by(Task.updated_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await session.execute(query)
    return result.scalars().all()


@router.get("/tasks/stats", response_model=TaskStats, tags=["任务"])
async def get_task_stats(session: AsyncSession = Depends(get_db_session)):
    """获取任务统计"""
    result = await session.execute(
        select(Task.status, func.count(Task.id)).group_by(Task.status)
    )
    stats = {row[0]: row[1] for row in result.all()}

    return TaskStats(
        total_count=sum(stats.values()),
        pending=stats.get("pending", 0),
        running=stats.get("running", 0),
        completed=stats.get("completed", 0),
        failed=stats.get("failed", 0),
        cancelled=stats.get("cancelled", 0),
    )


@router.post("/tasks", response_model=TaskResponse, tags=["任务"])
async def create_task(
    data: TaskCreate,
    session: AsyncSession = Depends(get_db_session),
):
    """创建新任务"""
    if data.project_id:
        # 验证项目存在
        proj = await session.execute(select(Project).where(Project.id == data.project_id))
        if not proj.scalar_one_or_none():
            raise HTTPException(status_code=400, detail="项目不存在")

    # 合并 Danbooru 参数到 params
    task_params = dict(data.params) if data.params else {}
    if data.danbooru_ids:
        task_params["danbooru_ids"] = data.danbooru_ids
    if data.tag_filter:
        task_params["tag_filter"] = data.tag_filter
    if data.limit and data.limit != 100:
        task_params["limit"] = data.limit
    if data.start_id:
        task_params["start_id"] = data.start_id
    if data.end_id:
        task_params["end_id"] = data.end_id

    task = Task(
        name=data.name,
        task_type=data.task_type,
        project_id=data.project_id,
        params=task_params,
        status="pending",
    )
    session.add(task)
    await session.flush()
    await session.refresh(task)
    return task


@router.get("/tasks/{task_id}", response_model=TaskDetailResponse, tags=["任务"])
async def get_task(task_id: int, session: AsyncSession = Depends(get_db_session)):
    """获取任务详情"""
    result = await session.execute(
        select(Task)
        .options(selectinload(Task.logs))
        .where(Task.id == task_id)
    )
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    # 统计帖子数
    posts_result = await session.execute(
        select(func.count(Post.id)).where(Post.task_id == task_id)
    )
    post_count = posts_result.scalar() or 0

    return TaskDetailResponse(
        **TaskResponse.model_validate(task).model_dump(),
        logs=[TaskLogResponse.model_validate(l) for l in task.logs],
        post_count=post_count,
    )


@router.delete("/tasks/{task_id}", response_model=MessageResponse, tags=["任务"])
async def delete_task(task_id: int, session: AsyncSession = Depends(get_db_session)):
    """删除任务"""
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status == "running":
        raise HTTPException(status_code=400, detail="任务正在运行中，请先停止")

    await session.delete(task)
    return MessageResponse(message="任务已删除")


@router.post("/tasks/{task_id}/start", response_model=TaskResponse, tags=["任务"])
async def start_task(
    task_id: int,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_db_session),
):
    """启动任务"""
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status not in ("pending", "paused", "failed"):
        raise HTTPException(status_code=400, detail=f"任务状态为 {task.status}，无法启动")

    task.status = "running"
    task.started_at = task.started_at or datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await session.flush()
    await session.refresh(task)

    background_tasks.add_task(run_task_background, task.id)

    return task


@router.post("/tasks/{task_id}/pause", response_model=TaskResponse, tags=["任务"])
async def pause_task(task_id: int, session: AsyncSession = Depends(get_db_session)):
    """暂停任务"""
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status != "running":
        raise HTTPException(status_code=400, detail="只有运行中的任务可以暂停")

    task.status = "paused"
    task.updated_at = datetime.utcnow()
    await session.flush()
    await session.refresh(task)
    return task


@router.post("/tasks/{task_id}/stop", response_model=TaskResponse, tags=["任务"])
async def stop_task(task_id: int, session: AsyncSession = Depends(get_db_session)):
    """停止任务"""
    result = await session.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if not task:
        raise HTTPException(status_code=404, detail="任务不存在")

    if task.status not in ("running", "paused"):
        raise HTTPException(status_code=400, detail="任务未在运行")

    task.status = "cancelled"
    task.completed_at = datetime.utcnow()
    task.updated_at = datetime.utcnow()
    await session.flush()
    await session.refresh(task)
    return task


@router.get("/tasks/{task_id}/logs", response_model=List[TaskLogResponse], tags=["任务"])
async def get_task_logs(
    task_id: int,
    level: Optional[str] = Query(None, description="日志级别"),
    limit: int = Query(100, ge=1, le=1000),
    session: AsyncSession = Depends(get_db_session),
):
    """获取任务日志"""
    result = await session.execute(select(Task).where(Task.id == task_id))
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="任务不存在")

    query = select(TaskLog).where(TaskLog.task_id == task_id)
    if level:
        query = query.where(TaskLog.level == level)
    query = query.order_by(TaskLog.created_at.desc()).limit(limit)

    result = await session.execute(query)
    return result.scalars().all()


# ========== 帖子查询 ==========

@router.get("/posts", response_model=PostListResponse, tags=["帖子"])
async def list_posts(
    task_id: Optional[int] = Query(None, description="任务ID"),
    tag: Optional[str] = Query(None, description="标签筛选"),
    min_score: Optional[int] = Query(None, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """列出帖子"""
    query = select(Post)
    count_query = select(func.count(Post.id))

    if task_id:
        query = query.where(Post.task_id == task_id)
        count_query = count_query.where(Post.task_id == task_id)

    if tag:
        # 简单标签筛选（包含匹配）
        query = query.where(Post.tag_string.ilike(f"%{tag}%"))
        count_query = count_query.where(Post.tag_string.ilike(f"%{tag}%"))

    if min_score is not None:
        query = query.where(Post.score >= min_score)
        count_query = count_query.where(Post.score >= min_score)

    # 总数
    total = (await session.execute(count_query)).scalar() or 0

    # 分页
    query = query.order_by(Post.fetched_at.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await session.execute(query)
    items = result.scalars().all()

    return PostListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_more=(page * page_size) < total,
    )


@router.get("/posts/stats", tags=["帖子"])
async def get_post_stats(session: AsyncSession = Depends(get_db_session)):
    """获取帖子统计"""
    result = await session.execute(
        select(
            func.count(Post.id),
            func.sum(Post.file_size),
            func.avg(Post.score),
        )
    )
    row = result.one()
    return {
        "total_posts": row[0] or 0,
        "total_bytes": row[1] or 0,
        "avg_score": round(float(row[2] or 0), 2),
    }


@router.get("/posts/{post_id}", response_model=PostResponse, tags=["帖子"])
async def get_post(post_id: int, session: AsyncSession = Depends(get_db_session)):
    """获取帖子详情"""
    result = await session.execute(select(Post).where(Post.id == post_id))
    post = result.scalar_one_or_none()
    if not post:
        raise HTTPException(status_code=404, detail="帖子不存在")
    return post


# ========== 标签查询 ==========

@router.get("/tags", response_model=TagListResponse, tags=["标签"])
async def list_tags(
    category: Optional[str] = Query(None, description="标签类别"),
    name: Optional[str] = Query(None, description="标签名模糊匹配"),
    min_count: int = Query(0, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    session: AsyncSession = Depends(get_db_session),
):
    """列出标签"""
    query = select(Tag)
    count_query = select(func.count(Tag.id))

    if category:
        query = query.where(Tag.category == category)
        count_query = count_query.where(Tag.category == category)

    if name:
        query = query.where(Tag.name.ilike(f"%{name}%"))
        count_query = count_query.where(Tag.name.ilike(f"%{name}%"))

    if min_count > 0:
        query = query.where(Tag.post_count >= min_count)
        count_query = count_query.where(Tag.post_count >= min_count)

    total = (await session.execute(count_query)).scalar() or 0

    query = query.order_by(Tag.post_count.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await session.execute(query)
    return TagListResponse(items=result.scalars().all(), total=total)


@router.get("/tags/character", response_model=TagListResponse, tags=["标签"])
async def list_character_tags(
    min_count: int = Query(50, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
    session: AsyncSession = Depends(get_db_session),
):
    """列出角色标签"""
    query = select(Tag).where(Tag.category == "character")
    count_query = select(func.count(Tag.id)).where(Tag.category == "character")

    if min_count > 0:
        query = query.where(Tag.post_count >= min_count)
        count_query = count_query.where(Tag.post_count >= min_count)

    total = (await session.execute(count_query)).scalar() or 0

    query = query.order_by(Tag.post_count.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await session.execute(query)
    return TagListResponse(items=result.scalars().all(), total=total)


# ========== 角色分析 ==========

@router.get("/characters", response_model=CharacterListResponse, tags=["角色"])
async def list_characters(
    min_score: float = Query(0.0, ge=0),
    min_count: int = Query(0, ge=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    session: AsyncSession = Depends(get_db_session),
):
    """列出角色（分析结果）"""
    query = select(Character)
    count_query = select(func.count(Character.id))

    if min_score > 0:
        query = query.where(Character.popularity_score >= min_score)
        count_query = count_query.where(Character.popularity_score >= min_score)

    if min_count > 0:
        query = query.where(Character.total_post_count >= min_count)
        count_query = count_query.where(Character.total_post_count >= min_count)

    total = (await session.execute(count_query)).scalar() or 0

    query = query.order_by(Character.popularity_score.desc())
    query = query.offset((page - 1) * page_size).limit(page_size)

    result = await session.execute(query)
    characters = result.scalars().all()

    # 填充 copyrights
    items = []
    for char in characters:
        char_result = await session.execute(
            select(Character).where(Character.id == char.id)
        )
        char_obj = char_result.scalar_one()

        # 获取 tag 名称
        tag_result = await session.execute(select(Tag).where(Tag.id == char.tag_id))
        tag = tag_result.scalar_one_or_none()
        character_tag = tag.name if tag else ""

        # 获取 copyrights
        cr_result = await session.execute(
            select(Copyright)
            .join(CharacterCopyright, CharacterCopyright.copyright_tag_id == Copyright.tag_id)
            .where(CharacterCopyright.character_tag_id == char.tag_id)
        )
        copyrights = []
        for cr in cr_result.scalars().all():
            cr_tag = await session.execute(select(Tag).where(Tag.id == cr.tag_id))
            cr_tag_obj = cr_tag.scalar_one_or_none()
            if cr_tag_obj:
                copyrights.append(cr_tag_obj.name)

        items.append(CharacterResponse(
            id=char.id,
            tag_id=char.tag_id,
            character_tag=character_tag,
            total_post_count=char.total_post_count,
            recent_post_count=char.recent_post_count,
            popularity_score=char.popularity_score,
            copyrights=copyrights,
            first_seen_post_id=char.first_seen_post_id,
            first_seen_at=char.first_seen_at,
            character_age_days=char.character_age_days,
            recent_ratio=char.recent_ratio,
            growth_score=char.growth_score,
            birth_confidence=char.birth_confidence,
            lifecycle_notes=char.lifecycle_notes,
            stat_at=char.stat_at,
            updated_at=char.updated_at,
        ))

    return CharacterListResponse(items=items, total=total)


@router.get("/characters/top", response_model=CharacterExportResponse, tags=["角色"])
async def get_top_characters(
    n: int = Query(200, ge=1, le=1000, description="数量"),
    recent_months: int = Query(6, ge=1, le=24, description="统计月份"),
    min_count: int = Query(50, ge=0, description="最低帖子数"),
    session: AsyncSession = Depends(get_db_session),
):
    """获取 Top N 角色榜单"""
    payload = _load_export_payload(_recent_export_path())
    if payload and _payload_matches_recent_filters(payload, n, recent_months, min_count):
        characters = payload.get("characters", [])[:n]
        filters = dict(payload.get("filters", {}))
        filters["top_n"] = n
        return CharacterExportResponse(
            characters=characters,
            generated_at=_parse_export_datetime(payload.get("generated_at")),
            total_count=len(characters),
            filters=filters,
        )

    cutoff = datetime.utcnow() - timedelta(days=recent_months * 30)

    result = await session.execute(
        select(Character)
        .where(Character.total_post_count >= min_count)
        .order_by(Character.popularity_score.desc())
        .limit(n)
    )
    characters = result.scalars().all()

    items = []
    for char in characters:
        tag_result = await session.execute(select(Tag).where(Tag.id == char.tag_id))
        tag = tag_result.scalar_one_or_none()

        cr_result = await session.execute(
            select(Copyright)
            .join(CharacterCopyright, CharacterCopyright.copyright_tag_id == Copyright.tag_id)
            .where(CharacterCopyright.character_tag_id == char.tag_id)
        )
        copyrights = []
        for cr in cr_result.scalars().all():
            cr_tag = await session.execute(select(Tag).where(Tag.id == cr.tag_id))
            cr_tag_obj = cr_tag.scalar_one_or_none()
            if cr_tag_obj:
                copyrights.append(cr_tag_obj.name)

        items.append({
            "character_tag": tag.name if tag else "",
            "copyrights": copyrights,
            "post_count": char.total_post_count,
            "recent_post_count": char.recent_post_count,
            "popularity_score": round(char.popularity_score, 4),
        })

    return CharacterExportResponse(
        characters=items,
        generated_at=datetime.utcnow(),
        total_count=len(items),
        filters={
            "top_n": n,
            "recent_months": recent_months,
            "min_count": min_count,
        },
    )


# ========== 统计 & 仪表盘 ==========

@router.get("/stats", response_model=DashboardStats, tags=["统计"])
async def get_stats(session: AsyncSession = Depends(get_db_session)):
    """获取系统统计"""
    # 帖子统计
    posts_result = await session.execute(
        select(func.count(Post.id), func.sum(Post.file_size))
    )
    posts_row = posts_result.one()
    total_posts = posts_row[0] or 0
    total_bytes = posts_row[1] or 0

    # 任务统计
    task_result = await session.execute(
        select(Task.status, func.count(Task.id)).group_by(Task.status)
    )
    task_stats = {row[0]: row[1] for row in task_result.all()}
    total_tasks = sum(task_stats.values())

    # 最近活动（最近24小时的日志）
    cutoff = datetime.utcnow() - timedelta(hours=24)
    logs_result = await session.execute(
        select(TaskLog)
        .where(TaskLog.created_at >= cutoff)
        .order_by(TaskLog.created_at.desc())
        .limit(20)
    )
    recent_activity = [
        {"level": log.level, "message": log.message, "created_at": log.created_at.isoformat()}
        for log in logs_result.scalars().all()
    ]

    return DashboardStats(
        total_posts=total_posts,
        total_tasks=total_tasks,
        running_tasks=task_stats.get("running", 0),
        completed_tasks=task_stats.get("completed", 0),
        failed_tasks=task_stats.get("failed", 0),
        pending_tasks=task_stats.get("pending", 0),
        total_download_bytes=total_bytes,
        recent_activity=recent_activity,
    )


@router.get("/export/characters", tags=["导出"])
async def export_characters(
    n: int = Query(200, ge=1, le=1000),
    recent_months: int = Query(6, ge=1, le=24),
    min_count: int = Query(50, ge=0),
    format: str = Query("json", pattern="^(json|csv)$"),
    session: AsyncSession = Depends(get_db_session),
):
    """导出角色榜单"""
    payload = _load_export_payload(_recent_export_path())
    if payload and _payload_matches_recent_filters(payload, n, recent_months, min_count):
        characters = payload.get("characters", [])[:n]
        filters = dict(payload.get("filters", {}))
        filters["top_n"] = n
        if format == "csv":
            import csv
            import io
            from fastapi.responses import StreamingResponse
            fieldnames = [
                "rank",
                "character_tag",
                "copyrights",
                "post_count",
                "recent_post_count",
                "popularity_score",
                "copyright_confidence",
                "needs_review",
                "notes",
            ]
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            for item in characters:
                row = {key: item.get(key) for key in fieldnames}
                row["copyrights"] = "|".join(item.get("copyrights") or [])
                writer.writerow(row)
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=character_list_recent_6m_top_200.csv"},
            )
        return {
            "characters": characters,
            "generated_at": payload.get("generated_at") or datetime.utcnow().isoformat(),
            "total_count": len(characters),
            "filters": filters,
        }

    # 复用 characters/top 逻辑
    result = await session.execute(
        select(Character)
        .where(Character.total_post_count >= min_count)
        .order_by(Character.popularity_score.desc())
        .limit(n)
    )
    characters = result.scalars().all()

    items = []
    for char in characters:
        tag_result = await session.execute(select(Tag).where(Tag.id == char.tag_id))
        tag = tag_result.scalar_one_or_none()

        cr_result = await session.execute(
            select(Copyright)
            .join(CharacterCopyright, CharacterCopyright.copyright_tag_id == Copyright.tag_id)
            .where(CharacterCopyright.character_tag_id == char.tag_id)
        )
        copyrights = []
        for cr in cr_result.scalars().all():
            cr_tag = await session.execute(select(Tag).where(Tag.id == cr.tag_id))
            cr_tag_obj = cr_tag.scalar_one_or_none()
            if cr_tag_obj:
                copyrights.append(cr_tag_obj.name)

        items.append({
            "character_tag": tag.name if tag else "",
            "copyrights": copyrights,
            "post_count": char.total_post_count,
        })

    if format == "csv":
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["character_tag", "copyrights", "post_count"])
        writer.writeheader()
        for item in items:
            row = dict(item)
            row["copyrights"] = "|".join(item["copyrights"])
            writer.writerow(row)
        from fastapi.responses import StreamingResponse
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=characters.csv"},
        )

    return {
        "characters": items,
        "generated_at": datetime.utcnow().isoformat(),
        "total_count": len(items),
    }


# ========== 训练数据集 ==========

@router.post("/datasets/export", response_model=DatasetExportResponse, tags=["训练数据集"])
async def export_dataset(
    data: DatasetExportRequest,
    session: AsyncSession = Depends(get_db_session),
):
    """按角色导出训练数据目录：图片、txt caption、json 元数据。"""
    result = await export_character_dataset(
        session=session,
        character_tag=data.character_tag,
        limit=data.limit,
        min_score=data.min_score,
        rating=data.rating,
        include_artist=data.include_artist,
        download_images=data.download_images,
        clean_target_dir=data.clean_target_dir,
    )
    return DatasetExportResponse(**result)


@router.post("/characters/build", response_model=CharacterExportResponse, tags=["角色"])
async def build_top_characters(
    n: int = Query(200, ge=1, le=1000, description="数量"),
    recent_months: int = Query(6, ge=1, le=24, description="统计月份"),
    min_count: int = Query(50, ge=0, description="最低帖子数"),
    session: AsyncSession = Depends(get_db_session),
):
    """生成正式角色榜单并落盘 JSON/CSV。"""
    result = await build_character_ranking(
        session=session,
        output_root=Path(get_settings().OUTPUT_ROOT),
        recent_months=recent_months,
        top_n=n,
        min_post_count=min_count,
    )
    return CharacterExportResponse(
        characters=result["characters"],
        generated_at=result["generated_at"],
        total_count=result["total_count"],
        filters=result["filters"],
    )


@router.post("/characters/build-emerging", response_model=CharacterExportResponse, tags=["角色"])
async def build_emerging_characters(
    n: int = Query(200, ge=1, le=1000),
    min_count: int = Query(50, ge=0),
    min_recent_count: int = Query(10, ge=0),
    max_age_days: int = Query(1095, ge=1),
    session: AsyncSession = Depends(get_db_session),
):
    """生成新兴热门角色榜并落盘 JSON/CSV。"""
    result = await build_emerging_character_ranking(
        session=session,
        output_root=Path(get_settings().OUTPUT_ROOT),
        top_n=n,
        min_post_count=min_count,
        min_recent_count=min_recent_count,
        max_age_days=max_age_days,
    )
    return CharacterExportResponse(
        characters=result["characters"],
        generated_at=result["generated_at"],
        total_count=result["total_count"],
        filters=result["filters"],
    )


@router.get("/characters/emerging", response_model=CharacterExportResponse, tags=["角色"])
async def get_emerging_characters(
    n: int = Query(200, ge=1, le=1000),
    min_count: int = Query(50, ge=0),
    min_recent_count: int = Query(10, ge=0),
    max_age_days: int = Query(1095, ge=1),
    session: AsyncSession = Depends(get_db_session),
):
    """查询当前新兴热门角色榜。"""
    export_path = Path(get_settings().OUTPUT_ROOT) / "exports" / "character_list_emerging_6m_top_200.json"
    payload = _load_export_payload(export_path)
    if payload:
        payload = refresh_emerging_payload_ages(payload)
        filters = payload.get("filters", {})
        if (
            int(filters.get("top_n", n)) == n
            and int(filters.get("min_post_count", min_count)) == min_count
            and int(filters.get("min_recent_count", min_recent_count)) == min_recent_count
            and int(filters.get("max_age_days", max_age_days)) == max_age_days
        ):
            return CharacterExportResponse(
                characters=payload.get("characters", []),
                generated_at=datetime.fromisoformat(payload["generated_at"]),
                total_count=len(payload.get("characters", [])),
                filters=filters,
            )

    result = await build_emerging_character_ranking(
        session=session,
        output_root=Path(get_settings().OUTPUT_ROOT),
        top_n=n,
        min_post_count=min_count,
        min_recent_count=min_recent_count,
        max_age_days=max_age_days,
    )
    return CharacterExportResponse(
        characters=result["characters"],
        generated_at=result["generated_at"],
        total_count=result["total_count"],
        filters=result["filters"],
    )
