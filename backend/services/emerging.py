"""新兴热门角色榜服务。"""

from __future__ import annotations

import csv
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

from collections import Counter, defaultdict

from sqlalchemy import func, select
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from models import Character, Post, PostTag, Tag


def age_boost(age_days: int | None, max_age_days: int) -> float:
    if age_days is None:
        return 0.0
    if age_days <= 0:
        return 1.0
    if age_days >= max_age_days:
        return 0.0
    return round(1.0 - (age_days / max_age_days), 6)


def compute_age_days(first_seen_at, now: datetime) -> int | None:
    if first_seen_at is None:
        return None
    first_seen = first_seen_at
    if getattr(first_seen, "tzinfo", None) is not None:
        first_seen = first_seen.astimezone(UTC).replace(tzinfo=None)
    current = now
    if getattr(current, "tzinfo", None) is not None:
        current = current.astimezone(UTC).replace(tzinfo=None)
    return max((current - first_seen).days, 0)


def refresh_emerging_payload_ages(payload: dict, now: datetime | None = None) -> dict:
    """对缓存导出的新兴榜按当前时间重算年龄字段。"""
    current = now or datetime.utcnow()
    updated = dict(payload)
    chars = []
    for item in payload.get("characters", []):
        row = dict(item)
        first_seen = row.get("first_seen_at")
        if first_seen:
            try:
                row["character_age_days"] = compute_age_days(datetime.fromisoformat(first_seen), current)
            except Exception:
                pass
        chars.append(row)
    updated["characters"] = chars
    updated["age_computed_at"] = current.isoformat()
    return updated


async def build_emerging_character_ranking(
    session: AsyncSession,
    output_root: Path,
    top_n: int = 200,
    min_post_count: int = 50,
    min_recent_count: int = 10,
    max_age_days: int = 730,
    stat_at: datetime | None = None,
) -> Dict[str, Any]:
    now = stat_at or datetime.utcnow()
    # Recent-window candidate pool: all active character tags seen in recent posts.
    recent_cutoff = datetime(now.year, now.month, now.day)  # normalized for reproducibility
    from datetime import timedelta
    recent_cutoff = recent_cutoff - timedelta(days=183)
    recent_rows = await session.execute(
        select(Tag.id, Tag.name, Tag.post_count, func.count(func.distinct(PostTag.post_id)).label("recent_count"))
        .join(PostTag, PostTag.tag_id == Tag.id)
        .join(Post, Post.id == PostTag.post_id)
        .where(Tag.category == "character", Post.created_at >= recent_cutoff)
        .group_by(Tag.id)
        .having(func.count(func.distinct(PostTag.post_id)) >= min_recent_count)
    )
    candidates_raw = list(recent_rows.all())
    candidate_ids = [int(row[0]) for row in candidates_raw]

    local_total_by_tag: Dict[int, int] = {}
    if candidate_ids:
        total_rows = await session.execute(
            select(PostTag.tag_id, func.count(func.distinct(PostTag.post_id)))
            .where(PostTag.tag_id.in_(candidate_ids))
            .group_by(PostTag.tag_id)
        )
        local_total_by_tag = {int(row[0]): int(row[1]) for row in total_rows.all()}

    character_rows = await session.execute(select(Character).where(Character.tag_id.in_(candidate_ids)))
    character_by_tag = {int(c.tag_id): c for c in character_rows.scalars().all()}

    candidates: List[Dict[str, Any]] = []
    max_recent = max((int(row[3]) for row in candidates_raw), default=1)
    if candidate_ids:
        char_pt = aliased(PostTag)
        cr_pt = aliased(PostTag)
        cr_tag = aliased(Tag)
        cr_rows = await session.execute(
            select(char_pt.tag_id, cr_tag.name, func.count(func.distinct(char_pt.post_id)))
            .join(Post, Post.id == char_pt.post_id)
            .join(cr_pt, cr_pt.post_id == char_pt.post_id)
            .join(cr_tag, cr_tag.id == cr_pt.tag_id)
            .where(
                char_pt.tag_id.in_(candidate_ids),
                Post.created_at >= recent_cutoff,
                cr_tag.category == "copyright",
            )
            .group_by(char_pt.tag_id, cr_tag.id)
        )
        copyright_by_tag: Dict[int, Counter] = defaultdict(Counter)
        for tag_id, cr_name, count in cr_rows.all():
            copyright_by_tag[int(tag_id)][cr_name] += int(count)
    else:
        copyright_by_tag = {}

    for tag_id, tag_name, global_post_count, recent_count in candidates_raw:
        total_count = max(int(global_post_count or 0), local_total_by_tag.get(int(tag_id), 0))
        if total_count < min_post_count:
            continue
        character = character_by_tag.get(int(tag_id))
        if character is None:
            continue
        age_days = compute_age_days(character.first_seen_at, now)
        if age_days is None or age_days > max_age_days:
            continue
        recent_ratio = (int(recent_count) / total_count) if total_count else 0.0
        score = (
            0.45 * (int(recent_count) / max_recent)
            + 0.30 * min(recent_ratio, 1.0)
            + 0.25 * age_boost(age_days, max_age_days)
        )
        candidates.append(
            {
                "character_tag": tag_name,
                "copyrights": [name for name, _ in copyright_by_tag.get(int(tag_id), Counter()).most_common(5)],
                "post_count": total_count,
                "recent_post_count": int(recent_count),
                "popularity_score": character.popularity_score,
                "first_seen_post_id": character.first_seen_post_id,
                "first_seen_at": character.first_seen_at.isoformat() if character.first_seen_at else None,
                "character_age_days": age_days,
                "recent_ratio": round(recent_ratio, 6),
                "growth_score": round(score, 6),
                "birth_confidence": round(character.birth_confidence or 0.0, 4),
                "lifecycle_notes": character.lifecycle_notes or "",
            }
        )

    candidates.sort(
        key=lambda x: (
            x["growth_score"],
            x["recent_post_count"],
            x["recent_ratio"],
            -x["character_age_days"],
            x["character_tag"],
        ),
        reverse=True,
    )
    items = candidates[:top_n]

    for idx, item in enumerate(items, 1):
        item["rank"] = idx

    export_dir = Path(output_root) / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    json_path = export_dir / "character_list_emerging_6m_top_200.json"
    csv_path = export_dir / "character_list_emerging_6m_top_200.csv"
    payload = {
        "generated_at": now.isoformat(),
        "age_computed_at": now.isoformat(),
        "filters": {
            "top_n": top_n,
            "min_post_count": min_post_count,
            "min_recent_count": min_recent_count,
            "max_age_days": max_age_days,
        },
        "characters": items,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    fieldnames = [
        "rank",
        "character_tag",
        "copyrights",
        "post_count",
        "recent_post_count",
        "first_seen_at",
        "character_age_days",
        "recent_ratio",
        "growth_score",
        "birth_confidence",
        "lifecycle_notes",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            row = {key: item.get(key) for key in fieldnames}
            row["copyrights"] = "|".join(item.get("copyrights") or [])
            writer.writerow(row)

    return {
        "characters": items,
        "generated_at": now,
        "total_count": len(items),
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "filters": payload["filters"],
    }
