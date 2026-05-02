"""元数据入库与角色榜单分析。"""

from __future__ import annotations

import csv
import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Character, CharacterCopyright, Copyright, Post, PostTag, Tag, TagAlias, TagImplication


TAG_FIELDS = {
    "general": "tag_string_general",
    "character": "tag_string_character",
    "copyright": "tag_string_copyright",
    "artist": "tag_string_artist",
    "meta": "tag_string_meta",
}


def split_tags(value: Optional[str]) -> List[str]:
    """Danbooru tag 字符串按空白分割，过滤空值。"""
    if not value:
        return []
    return [tag.strip() for tag in str(value).split() if tag.strip()]


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is not None:
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def grouped_tags(post: Dict[str, Any]) -> Dict[str, List[str]]:
    """从 Danbooru 元数据提取分组标签。"""
    groups = {category: split_tags(post.get(field)) for category, field in TAG_FIELDS.items()}

    # 兼容少量旧数据或手工数据里带 category:tag 前缀的格式。
    if not any(groups.values()):
        for raw in split_tags(post.get("tag_string")):
            if ":" not in raw:
                continue
            category, name = raw.split(":", 1)
            if category in groups and name:
                groups[category].append(name)

    return groups


async def get_or_create_tag(
    session: AsyncSession,
    name: str,
    category: str,
    post_count: int = 0,
) -> Tag:
    result = await session.execute(select(Tag).where(Tag.name == name))
    tag = result.scalar_one_or_none()
    if tag is None:
        tag = Tag(name=name, category=category, post_count=max(0, int(post_count or 0)))
        session.add(tag)
        await session.flush()
    else:
        if category and tag.category != category:
            tag.category = category
        if post_count and post_count > tag.post_count:
            tag.post_count = int(post_count)
    return tag


async def upsert_post_from_dict(session: AsyncSession, data: Dict[str, Any], task_id: Optional[int]) -> bool:
    """写入或更新单条帖子，返回是否新增。"""
    post_id = data.get("id")
    if post_id is None:
        return False
    post_id = int(post_id)

    result = await session.execute(select(Post).where(Post.id == post_id))
    post = result.scalar_one_or_none()
    is_new = post is None
    if post is None:
        post = Post(id=post_id)
        session.add(post)

    if task_id is not None:
        post.task_id = task_id
    post.md5 = data.get("md5")
    post.file_url = data.get("file_url")
    post.preview_url = data.get("preview_url")
    post.sample_url = data.get("sample_url")
    post.source = data.get("source")
    post.uploader_id = data.get("uploader_id")
    post.uploader_name = data.get("uploader_name")
    post.tag_string = data.get("tag_string")
    post.tag_count = int(data.get("tag_count") or 0)
    post.file_ext = data.get("file_ext")
    post.file_size = data.get("file_size")
    post.image_width = data.get("image_width")
    post.image_height = data.get("image_height")
    post.score = int(data.get("score") or 0)
    post.fav_count = int(data.get("fav_count") or 0)
    post.rating = data.get("rating")
    post.sources = data.get("sources") or ([data["source"]] if data.get("source") else [])
    post.has_children = bool(data.get("has_children", False))
    post.is_deleted = bool(data.get("is_deleted", False))
    post.is_flagged = bool(data.get("is_flagged", False))
    post.created_at = parse_datetime(data.get("created_at"))

    await session.flush()
    await session.execute(delete(PostTag).where(PostTag.post_id == post_id))

    groups = grouped_tags(data)
    touched_tag_ids: List[int] = []
    for category, names in groups.items():
        for name in dict.fromkeys(names):
            tag = await get_or_create_tag(session, name, category)
            touched_tag_ids.append(tag.id)
            session.add(PostTag(post_id=post_id, tag_id=tag.id))

    await session.flush()

    for tag_id in set(touched_tag_ids):
        count = await session.execute(
            select(func.count(PostTag.post_id)).where(PostTag.tag_id == tag_id)
        )
        observed = int(count.scalar() or 0)
        tag = await session.get(Tag, tag_id)
        if tag and observed > tag.post_count:
            tag.post_count = observed

    return is_new


async def import_jsonl(session: AsyncSession, jsonl_path: Path, task_id: Optional[int]) -> Dict[str, int]:
    """导入 Worker 产出的 JSONL 元数据。"""
    imported = 0
    created = 0
    errors = 0
    if not jsonl_path.exists():
        return {"imported": 0, "created": 0, "errors": 1}

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                created += int(await upsert_post_from_dict(session, data, task_id))
                imported += 1
            except Exception:
                errors += 1

    return {"imported": imported, "created": created, "errors": errors}


async def import_danbooru_tags(
    session: AsyncSession,
    tags: Iterable[Any],
) -> Dict[str, int]:
    imported = 0
    for item in tags:
        await get_or_create_tag(session, item.name, item.category_name, item.post_count)
        imported += 1
    return {"imported": imported}


async def import_danbooru_aliases(session: AsyncSession, aliases: Iterable[Any]) -> Dict[str, int]:
    imported = 0
    for item in aliases:
        alias = await session.get(TagAlias, int(item.id))
        if alias is None:
            alias = TagAlias(id=int(item.id))
            session.add(alias)
        alias.antecedent_name = item.antecedent_name
        alias.consequent_name = item.consequent_name
        alias.status = item.status
        imported += 1
    await session.flush()
    return {"imported": imported}


async def import_danbooru_implications(session: AsyncSession, implications: Iterable[Any]) -> Dict[str, int]:
    imported = 0
    for item in implications:
        implication = await session.get(TagImplication, int(item.id))
        if implication is None:
            implication = TagImplication(id=int(item.id))
            session.add(implication)
        implication.antecedent_name = item.antecedent_name
        implication.consequent_name = item.consequent_name
        implication.status = item.status
        imported += 1
    await session.flush()
    return {"imported": imported}


async def _tag_name(session: AsyncSession, tag_id: int) -> str:
    tag = await session.get(Tag, tag_id)
    return tag.name if tag else ""


async def _copyrights_for_character(session: AsyncSession, character_tag_id: int, limit: int = 5) -> List[Tag]:
    post_rows = await session.execute(
        select(PostTag.post_id).where(PostTag.tag_id == character_tag_id)
    )
    post_ids = [row[0] for row in post_rows.all()]
    if not post_ids:
        return []

    rows = await session.execute(
        select(Tag, func.count(PostTag.post_id).label("co_count"))
        .join(PostTag, PostTag.tag_id == Tag.id)
        .where(PostTag.post_id.in_(post_ids), Tag.category == "copyright")
        .group_by(Tag.id)
        .order_by(func.count(PostTag.post_id).desc())
        .limit(limit)
    )
    return [row[0] for row in rows.all()]


async def analyze_characters(
    session: AsyncSession,
    output_root: Path,
    min_post_count: int = 50,
    recent_months: int = 6,
    top_n: int = 200,
) -> Dict[str, Any]:
    """基于已入库标签生成角色榜单，并落盘 JSON/CSV 交付物。"""
    cutoff = datetime.now(UTC) - timedelta(days=recent_months * 30)
    tag_rows = await session.execute(
        select(Tag).where(Tag.category == "character", Tag.post_count >= min_post_count)
    )
    character_tags = list(tag_rows.scalars().all())

    scored: List[Dict[str, Any]] = []
    for tag in character_tags:
        observed_total = await session.execute(
            select(func.count(PostTag.post_id)).where(PostTag.tag_id == tag.id)
        )
        observed_total_count = int(observed_total.scalar() or 0)

        recent = await session.execute(
            select(func.count(PostTag.post_id))
            .join(Post, Post.id == PostTag.post_id)
            .where(PostTag.tag_id == tag.id, Post.created_at >= cutoff)
        )
        recent_count = int(recent.scalar() or 0)
        total_count = max(int(tag.post_count or 0), observed_total_count)
        if total_count < min_post_count:
            continue
        scored.append(
            {
                "tag": tag,
                "total_post_count": total_count,
                "recent_post_count": recent_count,
            }
        )

    max_total = max((item["total_post_count"] for item in scored), default=1)
    max_recent = max((item["recent_post_count"] for item in scored), default=0)
    for item in scored:
        total_norm = math.log1p(item["total_post_count"]) / math.log1p(max_total)
        recent_norm = (item["recent_post_count"] / max_recent) if max_recent else 0.0
        item["popularity_score"] = round(0.7 * total_norm + 0.3 * recent_norm, 6)

    scored.sort(key=lambda x: (x["popularity_score"], x["total_post_count"]), reverse=True)
    top = scored[:top_n]

    await session.execute(delete(CharacterCopyright))
    items: List[Dict[str, Any]] = []
    now = datetime.now(UTC)
    for item in top:
        tag: Tag = item["tag"]
        char_row = await session.execute(select(Character).where(Character.tag_id == tag.id))
        character = char_row.scalar_one_or_none()
        if character is None:
            character = Character(tag_id=tag.id)
            session.add(character)
        character.total_post_count = item["total_post_count"]
        character.recent_post_count = item["recent_post_count"]
        character.popularity_score = item["popularity_score"]
        character.stat_at = now

        copyrights = await _copyrights_for_character(session, tag.id)
        copyright_names: List[str] = []
        for cr_tag in copyrights:
            cr_row = await session.execute(select(Copyright).where(Copyright.tag_id == cr_tag.id))
            copyright = cr_row.scalar_one_or_none()
            if copyright is None:
                copyright = Copyright(tag_id=cr_tag.id, post_count=cr_tag.post_count)
                session.add(copyright)
                await session.flush()
            copyright_names.append(cr_tag.name)
            session.add(CharacterCopyright(character_tag_id=tag.id, copyright_tag_id=cr_tag.id))

        items.append(
            {
                "character_tag": tag.name,
                "copyrights": copyright_names,
                "post_count": item["total_post_count"],
                "recent_post_count": item["recent_post_count"],
                "popularity_score": item["popularity_score"],
            }
        )

    await session.flush()

    export_dir = Path(output_root) / "exports"
    export_dir.mkdir(parents=True, exist_ok=True)
    json_path = export_dir / "character_list_recent_6m_top_200.json"
    csv_path = export_dir / "character_list_recent_6m_top_200.csv"
    payload = {
        "generated_at": now.isoformat(),
        "filters": {
            "min_post_count": min_post_count,
            "recent_months": recent_months,
            "top_n": top_n,
        },
        "characters": items,
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["character_tag", "copyrights", "post_count", "recent_post_count", "popularity_score"],
        )
        writer.writeheader()
        for row in items:
            csv_row = dict(row)
            csv_row["copyrights"] = "|".join(row["copyrights"])
            writer.writerow(csv_row)

    return {
        "characters": items,
        "generated_at": now,
        "total_count": len(items),
        "json_path": str(json_path),
        "csv_path": str(csv_path),
    }
