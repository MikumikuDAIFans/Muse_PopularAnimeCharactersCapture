"""训练数据集导出与 caption 生成。"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from config import get_settings
from models import DownloadLog, Post, PostTag, Tag
from services.danbooru import get_danbooru_client
from services.rules import load_rules


SUBJECT_TAGS = {
    "1girl",
    "1boy",
    "2girls",
    "2boys",
    "3girls",
    "3boys",
    "solo",
    "multiple_girls",
    "multiple_boys",
    "multiple_views",
}


def safe_dirname(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.()!'-]+", "_", value).strip("_") or "unknown"


def infer_extension_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path
    suffix = Path(path).suffix.lower().lstrip(".")
    return suffix or fallback.lower()


async def post_tag_groups(session: AsyncSession, post_id: int) -> Dict[str, List[str]]:
    rules = load_rules()
    rows = await session.execute(
        select(Tag)
        .join(PostTag, PostTag.tag_id == Tag.id)
        .where(PostTag.post_id == post_id)
        .order_by(Tag.category, Tag.name)
    )
    groups = {
        "subject": [],
        "character": [],
        "copyright": [],
        "artist": [],
        "general": [],
        "other": [],
    }
    for tag in rows.scalars().all():
        if tag.category == "character":
            groups["character"].append(tag.name)
        elif tag.category == "copyright":
            groups["copyright"].append(tag.name)
        elif tag.category == "artist":
            groups["artist"].append(tag.name)
        elif tag.category == "general":
            if tag.name in rules.subject_tags:
                groups["subject"].append(tag.name)
            else:
                groups["general"].append(tag.name)
        else:
            groups["other"].append(tag.name)
    return groups


def build_caption(groups: Dict[str, List[str]], include_artist: bool = True) -> str:
    rules = load_rules()
    ordered: List[str] = []
    for key in ["subject", "character", "copyright"]:
        ordered.extend(groups.get(key, []))
    if include_artist:
        ordered.extend(groups.get("artist", []))
    ordered.extend(groups.get("general", []))
    ordered.extend(groups.get("other", []))
    return ", ".join(rules.clean_caption_tags(dict.fromkeys(tag for tag in ordered if tag)))


def _download_file(urls: List[str], dest_dir: Path, base_name: str, fallback_ext: str, expected_md5: Optional[str]) -> Dict[str, Any]:
    headers = {
        "User-Agent": get_settings().DANBOORU_USER_AGENT,
        "Referer": get_settings().DANBOORU_BASE_URL.rstrip("/") + "/",
    }
    last_error = None
    for url in [u for u in urls if u]:
        ext = infer_extension_from_url(url, fallback_ext)
        dest = dest_dir / f"{base_name}.{ext}"
        if dest.exists() and dest.stat().st_size > 0:
            return {"downloaded": False, "verified": _verify_md5(dest, expected_md5), "used_url": url, "dest_path": str(dest)}
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        try:
            with requests.get(url, stream=True, timeout=get_settings().DOWNLOAD_TIMEOUT, headers=headers) as resp:
                resp.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
            tmp.replace(dest)
            return {
                "downloaded": True,
                "verified": _verify_md5(dest, expected_md5),
                "used_url": url,
                "dest_path": str(dest),
            }
        except Exception as exc:
            last_error = exc
            if tmp.exists():
                tmp.unlink()
            continue
    raise RuntimeError(str(last_error) if last_error else "no downloadable url")


def _verify_md5(path: Path, expected_md5: Optional[str]) -> bool:
    if not expected_md5 or not get_settings().VERIFY_MD5:
        return True
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest().lower() == expected_md5.lower()


async def export_character_dataset(
    session: AsyncSession,
    character_tag: str,
    limit: int = 50,
    min_score: Optional[int] = None,
    rating: Optional[str] = None,
    include_artist: bool = True,
    download_images: bool = True,
    clean_target_dir: bool = True,
) -> Dict[str, Any]:
    settings = get_settings()
    output_dir = Path(settings.OUTPUT_ROOT) / "dataset" / safe_dirname(character_tag)
    if clean_target_dir and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    tag_row = await session.execute(
        select(Tag).where(Tag.name == character_tag, Tag.category == "character")
    )
    tag = tag_row.scalar_one_or_none()
    if tag is None:
        return {"character_tag": character_tag, "exported_count": 0, "dataset_dir": str(output_dir), "errors": []}

    query = (
        select(Post)
        .join(PostTag, PostTag.post_id == Post.id)
        .where(PostTag.tag_id == tag.id)
        .order_by(Post.score.desc(), Post.id.desc())
        .limit(limit)
    )
    if min_score is not None:
        query = query.where(Post.score >= min_score)
    if rating:
        query = query.where(Post.rating == rating)
    rows = await session.execute(query)
    posts = rows.scalars().all()

    exported = 0
    errors: List[Dict[str, Any]] = []
    for post in posts:
        groups = await post_tag_groups(session, post.id)
        caption = build_caption(groups, include_artist=include_artist)
        ext = (post.file_ext or "jpg").lower()
        image_path = output_dir / f"{post.id}.{ext}"
        txt_path = output_dir / f"{post.id}.txt"
        json_path = output_dir / f"{post.id}.json"

        image_urls = [post.file_url, post.sample_url, post.preview_url]
        # 对于早先入库但未保存 sample/preview 的帖子，按需向 Danbooru 补查一次。
        if download_images and (not post.sample_url and not post.preview_url):
            try:
                live_post = await asyncio.to_thread(get_danbooru_client().get_post, post.id)
                if live_post:
                    image_urls = [
                        live_post.file_url or post.file_url,
                        getattr(live_post, "large_file_url", None) or live_post.sample_url or post.sample_url,
                        getattr(live_post, "preview_file_url", None) or live_post.preview_url or post.preview_url,
                    ]
            except Exception:
                pass
        if download_images and any(image_urls):
            try:
                info = _download_file(image_urls, output_dir, str(post.id), ext, post.md5)
                post.file_path = info["dest_path"]
                post.file_verified = bool(info["verified"])
                session.add(DownloadLog(
                    post_id=post.id,
                    character_tag=character_tag,
                    status="success" if info["verified"] else "failed",
                    stage="download",
                    file_path=info["dest_path"],
                    error_message=None if info["verified"] else f"md5 verification failed via {info['used_url']}",
                ))
            except Exception as exc:
                errors.append({"post_id": post.id, "stage": "download", "error": str(exc)})
                session.add(DownloadLog(
                    post_id=post.id,
                    character_tag=character_tag,
                    status="failed",
                    stage="download",
                    file_path=str(image_path),
                    error_message=str(exc),
                ))
                continue
        elif not download_images:
            session.add(DownloadLog(
                post_id=post.id,
                character_tag=character_tag,
                status="skipped",
                stage="download",
                file_path=str(image_path),
                error_message="download disabled",
            ))

        txt_path.write_text(caption, encoding="utf-8")
        meta = {
            "post_id": post.id,
            "file_url": post.file_url,
            "sample_url": post.sample_url,
            "preview_url": post.preview_url,
            "source_url": post.source or (post.sources[0] if post.sources else None),
            "width": post.image_width,
            "height": post.image_height,
            "file_ext": post.file_ext,
            "rating": post.rating,
            "score": post.score,
            "fav_count": post.fav_count,
            "created_at": post.created_at.isoformat() if post.created_at else None,
            "raw_tag_string": post.tag_string,
            "tag_groups": groups,
            "caption_v1": caption,
        }
        json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        exported += 1

    return {
        "character_tag": character_tag,
        "exported_count": exported,
        "dataset_dir": str(output_dir),
        "errors": errors,
    }
