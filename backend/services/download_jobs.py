"""榜单驱动下载任务服务。"""

from __future__ import annotations

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import DownloadJob, DownloadJobItem, RankingSnapshot, RankingSnapshotItem


async def create_download_job_from_snapshot(
    session: AsyncSession,
    ranking_type: str,
    name: str,
    target_count: int,
    snapshot_id: int | None = None,
) -> dict[str, int]:
    """从榜单快照创建定向下载任务。"""
    if snapshot_id is None:
        snapshot = (
            await session.execute(
                select(RankingSnapshot)
                .where(RankingSnapshot.ranking_type == ranking_type)
                .order_by(desc(RankingSnapshot.generated_at), desc(RankingSnapshot.id))
                .limit(1)
            )
        ).scalar_one_or_none()
    else:
        snapshot = (
            await session.execute(select(RankingSnapshot).where(RankingSnapshot.id == snapshot_id))
        ).scalar_one_or_none()
    if snapshot is None:
        raise ValueError(f"ranking snapshot not found: {ranking_type}")

    rows = (
        await session.execute(
            select(RankingSnapshotItem)
            .where(RankingSnapshotItem.snapshot_id == snapshot.id)
            .order_by(RankingSnapshotItem.rank)
        )
    ).scalars().all()
    job = DownloadJob(
        source_snapshot_id=snapshot.id,
        name=name,
        status="pending",
        params={"ranking_type": ranking_type, "target_count": target_count},
    )
    session.add(job)
    await session.flush()
    for row in rows:
        session.add(
            DownloadJobItem(
                job_id=job.id,
                character_tag_id=row.character_tag_id,
                character_tag=row.character_tag,
                target_count=target_count,
                status="pending",
            )
        )
    await session.flush()
    return {"download_job_id": int(job.id), "items": len(rows)}
