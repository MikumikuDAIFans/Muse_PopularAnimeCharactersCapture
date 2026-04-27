"""数据库聚合层构建服务。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def rebuild_character_monthly_aggregates(session: AsyncSession) -> dict[str, Any]:
    """重建角色月度统计和角色-作品月度共现表。"""
    await session.flush()

    await session.execute(text("delete from character_monthly_copyright"))
    await session.execute(text("delete from character_monthly_stats"))

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
            where t.category = 'character' and p.created_at is not null
            group by t.id, {month_expr}
            """
        )
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
                and p.created_at is not null
            group by char_tag.id, copyright_tag.id, {month_expr}
            """
        )
    )

    stats_count = int(
        (await session.execute(text("select count(*) from character_monthly_stats"))).scalar() or 0
    )
    copyright_count = int(
        (await session.execute(text("select count(*) from character_monthly_copyright"))).scalar() or 0
    )
    return {
        "character_monthly_stats": stats_count,
        "character_monthly_copyright": copyright_count,
    }
