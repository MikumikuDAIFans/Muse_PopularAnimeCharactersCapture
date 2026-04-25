"""帖子爬虫 Worker。

该 Worker 只负责稳定抓取 Danbooru 元数据并写入 JSONL/检查点；
数据库入库由 runner 在 Worker 完成后统一执行，避免线程里混用异步 Session。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

from services.danbooru import DanbooruClient, DanbooruPost, get_danbooru_client
from workers.base import BaseWorker, WorkerConfig


class PostCrawlerWorker(BaseWorker):
    """帖子元数据爬虫 Worker。"""

    POSTS_LIMIT = 200

    def __init__(
        self,
        task_id: int,
        output_dir: Path,
        start_id: Optional[int] = None,
        end_id: Optional[int] = None,
        tags: Optional[List[str]] = None,
        tag_filter: Optional[str] = None,
        danbooru_ids: Optional[List[int]] = None,
        limit: Optional[int] = None,
        resume: bool = False,
        config: Optional[WorkerConfig] = None,
    ) -> None:
        super().__init__(config)
        self.task_id = task_id
        self.start_id = start_id
        self.end_id = end_id
        self.output_dir = Path(output_dir)
        self.tags = [t for t in (tags or []) if t]
        self.tag_filter = (tag_filter or "").strip()
        self.danbooru_ids = [int(i) for i in (danbooru_ids or [])]
        self.limit = int(limit or 0) or None
        self.resume = bool(resume)
        self._client: Optional[DanbooruClient] = None
        self.result: Dict[str, Any] = {}

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / f"task_{self.task_id}_posts.jsonl"
        self.invalid_file = self.output_dir / f"task_{self.task_id}_posts.invalid.jsonl"
        self.checkpoint_file = self.output_dir / f"task_{self.task_id}.checkpoint.json"

    @property
    def client(self) -> DanbooruClient:
        if self._client is None:
            self._client = get_danbooru_client()
        return self._client

    def _load_seen_ids(self) -> Set[int]:
        seen: Set[int] = set()
        if not self.output_file.exists():
            return seen
        with open(self.output_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get("id") is not None:
                    seen.add(int(item["id"]))
        return seen

    def _write_checkpoint(self, **data: Any) -> None:
        payload = {"task_id": self.task_id, **data}
        tmp = self.checkpoint_file.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.checkpoint_file)

    def _load_checkpoint(self) -> Dict[str, Any]:
        if not self.resume or not self.checkpoint_file.exists():
            return {}
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        if int(data.get("task_id") or 0) != self.task_id:
            return {}
        return data

    def _append_posts(self, posts: Iterable[DanbooruPost], seen: Set[int]) -> int:
        written = 0
        with open(self.output_file, "a", encoding="utf-8") as f:
            for post in posts:
                if post.id in seen:
                    continue
                seen.add(post.id)
                f.write(json.dumps(post.to_dict(), ensure_ascii=False) + "\n")
                written += 1
        return written

    def _all_search_tags(self) -> List[str]:
        tags = list(self.tags)
        if self.tag_filter:
            tags.extend(self.tag_filter.split())
        return tags

    def _crawl_ids(self, seen: Set[int]) -> Dict[str, Any]:
        total = len(self.danbooru_ids)
        written = 0
        errors = 0
        checkpoint = self._load_checkpoint()
        checkpoint_index = int(checkpoint.get("processed_count") or 0)
        start_index = checkpoint_index if self.resume else 0
        if start_index:
            self.update_progress(
                "crawler",
                processed=start_index,
                total=total,
                status="running",
                message=f"从 checkpoint 恢复，跳过已处理 ID 数 {start_index}",
            )
        last_idx = start_index
        for idx, post_id in enumerate(self.danbooru_ids[start_index:], start_index + 1):
            last_idx = idx
            if self._stop_event.is_set():
                break
            if post_id in seen:
                self.update_progress("crawler", processed=idx, total=total, status="running")
                continue
            post = self.client.get_post(post_id)
            if post is None:
                errors += 1
                with open(self.invalid_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"id": post_id, "reason": "not_found"}, ensure_ascii=False) + "\n")
            else:
                written += self._append_posts([post], seen)
            self._write_checkpoint(last_post_id=post_id, processed_count=idx, error_count=errors)
            self.update_progress(
                "crawler",
                processed=idx,
                total=total,
                error_count=errors,
                status="running",
                message=f"已处理 ID {post_id}",
            )
        return {"written": written, "errors": errors, "processed": min(total, last_idx)}

    def _crawl_search(self, seen: Set[int]) -> Dict[str, Any]:
        tags = self._all_search_tags()
        checkpoint = self._load_checkpoint()
        checkpoint_cursor = checkpoint.get("last_cursor")
        cursor: Optional[int] = int(checkpoint_cursor) if checkpoint_cursor else ((self.end_id + 1) if self.end_id else None)
        written = 0
        errors = 0
        processed_pages = int(checkpoint.get("processed_pages") or 0) if checkpoint else 0
        max_items = self.limit or 0

        if checkpoint_cursor:
            self.update_progress(
                "crawler",
                processed=len(seen),
                total=max_items or 0,
                status="running",
                message=f"从 checkpoint 恢复，last_cursor={checkpoint_cursor}",
            )

        while not self._stop_event.is_set():
            page = f"b{cursor}" if cursor else None
            try:
                posts = self.client.get_posts(
                    limit=self.POSTS_LIMIT,
                    page=page,
                    tags=tags,
                    start_id=self.start_id,
                    end_id=self.end_id,
                )
            except Exception as exc:
                errors += 1
                with open(self.invalid_file, "a", encoding="utf-8") as f:
                    f.write(json.dumps({"page": page, "reason": str(exc)}, ensure_ascii=False) + "\n")
                break

            if not posts:
                break

            filtered: List[DanbooruPost] = []
            min_id = min(post.id for post in posts)
            for post in posts:
                if self.end_id and post.id > self.end_id:
                    continue
                if self.start_id and post.id < self.start_id:
                    continue
                filtered.append(post)
                if max_items and (written + len(filtered)) >= max_items:
                    break

            written += self._append_posts(filtered, seen)
            processed_pages += 1
            cursor = min_id
            self._write_checkpoint(last_cursor=cursor, processed_pages=processed_pages, written=written, error_count=errors)
            self.update_progress(
                "crawler",
                processed=written,
                total=max_items or 0,
                error_count=errors,
                status="running",
                message=f"已写入 {written} 条元数据",
            )

            if max_items and written >= max_items:
                break
            if self.start_id and min_id < self.start_id:
                break

        return {"written": written, "errors": errors, "processed_pages": processed_pages}

    def run(self) -> None:
        """执行爬取。"""
        self._stop_event.clear()
        self.update_progress("crawler", status="running", message="开始爬取")
        if not self.resume:
            for path in [self.output_file, self.invalid_file, self.checkpoint_file]:
                if path.exists():
                    path.unlink()
        seen = self._load_seen_ids()

        if self.danbooru_ids:
            stats = self._crawl_ids(seen)
        else:
            stats = self._crawl_search(seen)

        status = "stopped" if self._stop_event.is_set() else "completed"
        self.result = {
            **stats,
            "status": status,
            "output_file": str(self.output_file),
            "total_seen": len(seen),
        }
        self.update_progress(
            "crawler",
            processed=stats.get("written", 0),
            total=self.limit or stats.get("written", 0),
            error_count=stats.get("errors", 0),
            status=status,
            message=f"爬取{status}，本次写入 {stats.get('written', 0)} 条",
        )

    def stop(self) -> None:
        """停止爬虫。"""
        self._stop_event.set()
