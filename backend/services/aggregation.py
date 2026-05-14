"""数据库聚合层构建服务。"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def month_start(value: date | datetime) -> date:
    if isinstance(value, datetime):
        value = value.date()
    return date(value.year, value.month, 1)


async def rebuild_character_monthly_aggregates(
    session: AsyncSession,
    start_date: date | datetime | None = None,
    end_date: date | datetime | None = None,
) -> dict[str, Any]:
    """重建角色月度统计和角色-作品月度共现表。

    When a date window is provided, only months touched by that window are
    refreshed. This keeps the legacy full rebuild behavior while allowing the
    weekly pipeline to avoid deleting historical aggregate rows.
    """
    await session.flush()

    params: dict[str, Any] = {}
    month_filter = ""
    post_filter = "p.created_at is not null"
    mode = "full"
    if start_date and end_date:
        start_month = month_start(start_date)
        end_month = month_start(end_date)
        params = {"start_month": start_month, "end_month": end_month}
        month_filter = "where month_start between :start_month and :end_month"
        post_filter = (
            "p.created_at is not null "
            "and date_trunc('month', p.created_at)::date between :start_month and :end_month"
        )
        mode = "incremental"

    await session.execute(text(f"delete from character_monthly_copyright {month_filter}"), params)
    await session.execute(text(f"delete from character_monthly_stats {month_filter}"), params)

    month_expr = "date_trunc('month', p.created_at)::date"

    await session.execute(
        text(
            f"""
            insert into character_monthly_stats (
                character_tag_id,
                month_start,
                post_count,
                fav_count_sum,
                score_sum,
                first_post_id,
                first_seen_at,
                updated_at
            )
            select
                t.id as character_tag_id,
                {month_expr} as month_start,
                count(distinct p.id) as post_count,
                coalesce(sum(p.fav_count), 0) as fav_count_sum,
                coalesce(sum(p.score), 0) as score_sum,
                min(p.id) as first_post_id,
                min(p.created_at) as first_seen_at,
                current_timestamp as updated_at
            from tag t
            join post_tag pt on pt.tag_id = t.id
            join post p on p.id = pt.post_id
            where t.category = 'character' and {post_filter}
            group by t.id, {month_expr}
            """
        ),
        params,
    )

    await session.execute(
        text(
            f"""
            insert into character_monthly_copyright (
                character_tag_id,
                copyright_tag_id,
                month_start,
                post_count,
                updated_at
            )
            select
                char_tag.id as character_tag_id,
                copyright_tag.id as copyright_tag_id,
                {month_expr} as month_start,
                count(distinct p.id) as post_count,
                current_timestamp as updated_at
            from tag char_tag
            join post_tag char_pt on char_pt.tag_id = char_tag.id
            join post p on p.id = char_pt.post_id
            join post_tag copyright_pt on copyright_pt.post_id = p.id
            join tag copyright_tag on copyright_tag.id = copyright_pt.tag_id
            where
                char_tag.category = 'character'
                and copyright_tag.category = 'copyright'
                and {post_filter}
            group by char_tag.id, copyright_tag.id, {month_expr}
            """
        ),
        params,
    )

    stats_count = int(
        (
            await session.execute(
                text(f"select count(*) from character_monthly_stats {month_filter}"),
                params,
            )
        ).scalar()
        or 0
    )
    copyright_count = int(
        (
            await session.execute(
                text(f"select count(*) from character_monthly_copyright {month_filter}"),
                params,
            )
        ).scalar()
        or 0
    )
    return {
        "mode": mode,
        "character_monthly_stats": stats_count,
        "character_monthly_copyright": copyright_count,
    }
