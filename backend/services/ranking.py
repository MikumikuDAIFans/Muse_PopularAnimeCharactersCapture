"""正式角色榜单生成服务。"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import delete, func, select
from sqlalchemy.orm import aliased
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    Character,
    CharacterCopyright,
    Copyright,
    Post,
    PostTag,
    RankingSnapshot,
    RankingSnapshotItem,
    Tag,
    TagAlias,
)
from services.rules import RuleSet, load_rules

def recent_cutoff(months: int, now: datetime | None = None) -> datetime:
    base = now or datetime.now(UTC)
    return base - timedelta(days=months * 30)


async def alias_map(session: AsyncSession) -> Dict[str, str]:
    rows = await session.execute(select(TagAlias).where(TagAlias.status == "active"))
    return {item.antecedent_name: item.consequent_name for item in rows.scalars().all()}


async def copyright_counts_for_character(
    session: AsyncSession,
    character_tag_id: int,
    limit: int = 5,
) -> tuple[List[str], float]:
    post_rows = await session.execute(select(PostTag.post_id).where(PostTag.tag_id == character_tag_id))
    post_ids = [row[0] for row in post_rows.all()]
    if not post_ids:
        return [], 0.0

    rows = await session.execute(
        select(Tag.name, func.count(PostTag.post_id).label("count"))
        .join(PostTag, PostTag.tag_id == Tag.id)
        .where(PostTag.post_id.in_(post_ids), Tag.category == "copyright")
        .group_by(Tag.id)
        .order_by(func.count(PostTag.post_id).desc())
        .limit(limit)
    )
    pairs = [(row[0], int(row[1])) for row in rows.all()]
    total_refs = sum(count for _, count in pairs)
    confidence = min(total_refs / len(post_ids), 1.0) if post_ids else 0.0
    return [name for name, _ in pairs], round(confidence, 4)


async def build_character_ranking(
    session: AsyncSession,
    output_root: Path,
    recent_months: int = 6,
    top_n: int = 200,
    min_post_count: int = 50,
    require_recent: bool = True,
    rules: RuleSet | None = None,
    stat_at: datetime | None = None,
) -> Dict[str, Any]:
    """生成可复现角色榜单。

    排名分数采用任务文档推荐公式：
    0.7 * normalized_total_post_count + 0.3 * normalized_recent_post_count。
    """
    rules = rules or load_rules()
    now = stat_at or datetime.now(UTC)
    cutoff = recent_cutoff(recent_months, now)
    aliases = await alias_map(session)

    total_rows_result = await session.execute(
        select(Tag.id, Tag.name, Tag.post_count, func.count(func.distinct(PostTag.post_id)))
        .outerjoin(PostTag, PostTag.tag_id == Tag.id)
        .where(Tag.category == "character")
        .group_by(Tag.id)
    )
    total_rows = list(total_rows_result.all())

    recent_rows_result = await session.execute(
        select(Tag.id, func.count(func.distinct(PostTag.post_id)))
        .join(PostTag, PostTag.tag_id == Tag.id)
        .join(Post, Post.id == PostTag.post_id)
        .where(Tag.category == "character", Post.created_at >= cutoff)
        .group_by(Tag.id)
    )
    recent_by_tag = {int(row[0]): int(row[1]) for row in recent_rows_result.all()}

    grouped: Dict[str, Dict[str, Any]] = {}
    source_tag_ids: Dict[str, List[int]] = defaultdict(list)
    for tag_id, tag_name, tag_post_count, observed_total_count in total_rows:
        canonical = rules.canonical_character(tag_name, aliases)
        include, needs_review, note = rules.character_decision(canonical)
        if not include:
            continue
        bucket = grouped.setdefault(
            canonical,
            {
                "character_tag": canonical,
                "post_count": 0,
                "recent_post_count": 0,
                "needs_review": needs_review,
                "notes": [note] if note else [],
            },
        )
        bucket["post_count"] = max(bucket["post_count"], int(tag_post_count or 0), int(observed_total_count or 0))
        if canonical != tag_name:
            bucket["notes"].append(f"alias: {tag_name} -> {canonical}")
        source_tag_ids[canonical].append(int(tag_id))

    for canonical, tag_ids in source_tag_ids.items():
        grouped[canonical]["recent_post_count"] = sum(recent_by_tag.get(tag_id, 0) for tag_id in tag_ids)

    rows = [
        item for item in grouped.values()
        if item["post_count"] >= min_post_count
        and (not require_recent or item["recent_post_count"] > 0)
    ]
    max_total = max((item["post_count"] for item in rows), default=1)
    max_recent = max((item["recent_post_count"] for item in rows), default=0)

    for item in rows:
        total_norm = math.log1p(item["post_count"]) / math.log1p(max_total)
        recent_norm = item["recent_post_count"] / max_recent if max_recent else 0.0
        item["popularity_score"] = round(0.7 * total_norm + 0.3 * recent_norm, 6)

    rows.sort(
        key=lambda x: (
            x["popularity_score"],
            x["recent_post_count"],
            x["post_count"],
            x["character_tag"],
        ),
        reverse=True,
    )
    items = rows[:top_n]

    top_source_ids = [tag_id for item in items for tag_id in source_tag_ids[item["character_tag"]]]
    copyright_by_tag: Dict[int, Counter] = defaultdict(Counter)
    copyright_ref_by_tag: Dict[int, int] = defaultdict(int)
    if top_source_ids:
        char_pt = aliased(PostTag)
        cr_pt = aliased(PostTag)
        cr_tag = aliased(Tag)
        cr_rows = await session.execute(
            select(char_pt.tag_id, cr_tag.name, func.count(func.distinct(char_pt.post_id)))
            .join(Post, Post.id == char_pt.post_id)
            .join(cr_pt, cr_pt.post_id == char_pt.post_id)
            .join(cr_tag, cr_tag.id == cr_pt.tag_id)
            .where(
                char_pt.tag_id.in_(top_source_ids),
                Post.created_at >= cutoff,
                cr_tag.category == "copyright",
            )
            .group_by(char_pt.tag_id, cr_tag.id)
        )
        for tag_id, cr_name, count in cr_rows.all():
            copyright_by_tag[int(tag_id)][cr_name] += int(count)
            copyright_ref_by_tag[int(tag_id)] += int(count)

    for item in items:
        copyrights = Counter()
        best_confidence = 0.0
        for tag_id in source_tag_ids[item["character_tag"]]:
            copyrights.update(copyright_by_tag.get(tag_id, Counter()))
            recent_count = recent_by_tag.get(tag_id, 0)
            if recent_count:
                best_confidence = max(best_confidence, min(copyright_ref_by_tag.get(tag_id, 0) / recent_count, 1.0))
        item["copyrights"] = [name for name, _ in copyrights.most_common(5)]
        item["copyright_confidence"] = round(best_confidence, 4)
        if not item["copyrights"]:
            item["needs_review"] = True
            item["notes"].append("missing copyright")
        item["notes"] = "; ".join(dict.fromkeys(n for n in item["notes"] if n))

    for idx, item in enumerate(items, 1):
        item["rank"] = idx

    tag_id_by_character = {
        item["character_tag"]: source_tag_ids[item["character_tag"]][0]
        for item in items
        if source_tag_ids.get(item["character_tag"])
    }

    # Batch-fetch first-seen info for all top character tag IDs.
    all_top_tag_ids = [tid for item in items for tid in source_tag_ids.get(item["character_tag"], [])]
    first_seen_by_tag: Dict[int, tuple] = {}
    if all_top_tag_ids:
        first_seen_rows = await session.execute(
            select(PostTag.tag_id, func.min(Post.created_at), func.min(Post.id))
            .join(Post, Post.id == PostTag.post_id)
            .where(PostTag.tag_id.in_(all_top_tag_ids))
            .group_by(PostTag.tag_id)
        )
        for tag_id, min_created, min_post_id in first_seen_rows.all():
            first_seen_by_tag[int(tag_id)] = (min_created, int(min_post_id) if min_post_id else None)

    await session.execute(delete(CharacterCopyright))
    for item in items:
        tag_row = await session.execute(select(Tag).where(Tag.name == item["character_tag"], Tag.category == "character"))
        tag = tag_row.scalar_one_or_none()
        if tag is None:
            continue
        char_row = await session.execute(select(Character).where(Character.tag_id == tag.id))
        character = char_row.scalar_one_or_none()
        if character is None:
            character = Character(tag_id=tag.id)
            session.add(character)
        character.total_post_count = int(item["post_count"])
        character.recent_post_count = int(item["recent_post_count"])
        character.popularity_score = float(item["popularity_score"])
        character.stat_at = now
        # Populate first-seen info from batch query.
        fs = first_seen_by_tag.get(tag.id)
        if fs:
            character.first_seen_at = fs[0]
            character.first_seen_post_id = fs[1]
        for cr_name in item.get("copyrights") or []:
            cr_tag_row = await session.execute(select(Tag).where(Tag.name == cr_name, Tag.category == "copyright"))
            cr_tag = cr_tag_row.scalar_one_or_none()
            if cr_tag is None:
                continue
            copyright_row = await session.execute(select(Copyright).where(Copyright.tag_id == cr_tag.id))
            copyright = copyright_row.scalar_one_or_none()
            if copyright is None:
                copyright = Copyright(tag_id=cr_tag.id, post_count=cr_tag.post_count)
                session.add(copyright)
                await session.flush()
            session.add(CharacterCopyright(character_tag_id=tag.id, copyright_tag_id=cr_tag.id))
    await session.flush()

    export_dir = Path(output_root) / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    export_stem = f"character_list_recent_{recent_months}m_top_{top_n}"
    json_path = export_dir / f"{export_stem}.json"
    csv_path = export_dir / f"{export_stem}.csv"
    payload = {
        "generated_at": now.isoformat(),
        "stat_at": now.isoformat(),
        "filters": {
            "recent_months": recent_months,
            "top_n": top_n,
            "min_post_count": min_post_count,
            "require_recent": require_recent,
            "recent_window_start": cutoff.date().isoformat(),
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
        "popularity_score",
        "copyright_confidence",
        "needs_review",
        "notes",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for item in items:
            row = {key: item.get(key) for key in fieldnames}
            row["copyrights"] = "|".join(item.get("copyrights") or [])
            writer.writerow(row)

    snapshot = RankingSnapshot(
        ranking_type="recent",
        window_months=recent_months,
        top_n=top_n,
        min_post_count=min_post_count,
        filters=payload["filters"],
        generated_at=now,
        export_json_path=str(json_path),
        export_csv_path=str(csv_path),
    )
    session.add(snapshot)
    await session.flush()
    for item in items:
        tag_id = tag_id_by_character.get(item["character_tag"])
        if tag_id is None:
            continue
        session.add(
            RankingSnapshotItem(
                snapshot_id=snapshot.id,
                rank=int(item["rank"]),
                character_tag_id=int(tag_id),
                character_tag=item["character_tag"],
                post_count=int(item["post_count"]),
                recent_post_count=int(item["recent_post_count"]),
                popularity_score=float(item["popularity_score"]),
                growth_score=0.0,
                payload=item,
            )
        )
    await session.flush()

    return {
        "characters": items,
        "generated_at": now,
        "total_count": len(items),
        "json_path": str(json_path),
        "csv_path": str(csv_path),
        "filters": payload["filters"],
    }
