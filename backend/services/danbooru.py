"""Danbooru API 客户端服务

从原有的 danbooru_git.py 核心逻辑提取并适配为异步服务。
"""

import hashlib
import json
import time
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.parse import unquote

import requests
from requests.adapters import HTTPAdapter

from config import get_settings


@dataclass
class DanbooruPost:
    """Danbooru帖子对象"""
    id: int
    md5: Optional[str] = None
    file_url: Optional[str] = None
    file_ext: Optional[str] = None
    file_size: Optional[int] = None
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    preview_url: Optional[str] = None
    sample_url: Optional[str] = None
    large_file_url: Optional[str] = None
    preview_file_url: Optional[str] = None
    source: Optional[str] = None
    tag_string: Optional[str] = None
    tag_string_general: Optional[str] = None
    tag_string_character: Optional[str] = None
    tag_string_copyright: Optional[str] = None
    tag_string_artist: Optional[str] = None
    tag_string_meta: Optional[str] = None
    tag_count: int = 0
    score: int = 0
    fav_count: int = 0
    rating: Optional[str] = None
    uploader_id: Optional[int] = None
    uploader_name: Optional[str] = None
    sources: List[str] = field(default_factory=list)
    has_children: bool = False
    is_deleted: bool = False
    is_flagged: bool = False
    created_at: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DanbooruPost":
        """从API响应字典构建对象"""
        sources = data.get("sources") or []
        if isinstance(sources, str):
            sources = [sources]

        return cls(
            id=int(data["id"]),
            md5=data.get("md5"),
            file_url=data.get("file_url"),
            file_ext=data.get("file_ext"),
            file_size=data.get("file_size"),
            image_width=data.get("image_width"),
            image_height=data.get("image_height"),
            preview_url=data.get("preview_url"),
            sample_url=data.get("sample_url"),
            large_file_url=data.get("large_file_url"),
            preview_file_url=data.get("preview_file_url"),
            source=data.get("source"),
            tag_string=data.get("tag_string"),
            tag_string_general=data.get("tag_string_general"),
            tag_string_character=data.get("tag_string_character"),
            tag_string_copyright=data.get("tag_string_copyright"),
            tag_string_artist=data.get("tag_string_artist"),
            tag_string_meta=data.get("tag_string_meta"),
            tag_count=int(data.get("tag_count") or 0),
            score=int(data.get("score") or 0),
            fav_count=int(data.get("fav_count") or 0),
            rating=data.get("rating"),
            uploader_id=data.get("uploader_id"),
            uploader_name=data.get("uploader_name"),
            sources=sources,
            has_children=bool(data.get("has_children", False)),
            is_deleted=bool(data.get("is_deleted", False)),
            is_flagged=bool(data.get("is_flagged", False)),
            created_at=data.get("created_at"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "id": self.id,
            "md5": self.md5,
            "file_url": self.file_url,
            "file_ext": self.file_ext,
            "file_size": self.file_size,
            "image_width": self.image_width,
            "image_height": self.image_height,
            "preview_url": self.preview_url,
            "sample_url": self.sample_url,
            "large_file_url": self.large_file_url,
            "preview_file_url": self.preview_file_url,
            "source": self.source,
            "tag_string": self.tag_string,
            "tag_string_general": self.tag_string_general,
            "tag_string_character": self.tag_string_character,
            "tag_string_copyright": self.tag_string_copyright,
            "tag_string_artist": self.tag_string_artist,
            "tag_string_meta": self.tag_string_meta,
            "tag_count": self.tag_count,
            "score": self.score,
            "fav_count": self.fav_count,
            "rating": self.rating,
            "uploader_id": self.uploader_id,
            "uploader_name": self.uploader_name,
            "sources": self.sources,
            "has_children": self.has_children,
            "is_deleted": self.is_deleted,
            "is_flagged": self.is_flagged,
            "created_at": self.created_at,
        }


@dataclass
class DanbooruTag:
    """Danbooru标签对象"""
    id: int
    name: str
    category: int  # 0=general, 1=artist, 3=copyright, 4=character, 5=meta, 6=style
    post_count: int = 0

    CATEGORY_MAP = {
        0: "general",
        1: "artist",
        3: "copyright",
        4: "character",
        5: "meta",
        6: "style",
    }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DanbooruTag":
        return cls(
            id=int(data["id"]),
            name=data["name"],
            category=int(data.get("category") or 0),
            post_count=int(data.get("post_count") or 0),
        )

    @property
    def category_name(self) -> str:
        return self.CATEGORY_MAP.get(self.category, "general")


@dataclass
class DanbooruTagAlias:
    """Danbooru 标签 alias 对象。"""
    id: int
    antecedent_name: str
    consequent_name: str
    status: str = "active"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DanbooruTagAlias":
        return cls(
            id=int(data["id"]),
            antecedent_name=data["antecedent_name"],
            consequent_name=data["consequent_name"],
            status=data.get("status") or "active",
        )


@dataclass
class DanbooruTagImplication:
    """Danbooru 标签 implication 对象。"""
    id: int
    antecedent_name: str
    consequent_name: str
    status: str = "active"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DanbooruTagImplication":
        return cls(
            id=int(data["id"]),
            antecedent_name=data["antecedent_name"],
            consequent_name=data["consequent_name"],
            status=data.get("status") or "active",
        )


class RateLimiter:
    """线程安全的速率限制器"""

    def __init__(self, rate_per_sec: float) -> None:
        self.rate = float(rate_per_sec)
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        if self.rate <= 0:
            return
        interval = 1.0 / self.rate
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                sleep_s = self._next_allowed - now
                self._next_allowed += interval
            else:
                sleep_s = 0.0
                self._next_allowed = now + interval
        if sleep_s > 0:
            time.sleep(sleep_s)


class DanbooruClient:
    """Danbooru API客户端"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        login: Optional[str] = None,
        api_key: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        settings = get_settings()
        self.base_url = (base_url or settings.DANBOORU_BASE_URL).rstrip("/")
        self.login = login or settings.DANBOORU_LOGIN
        self.api_key = api_key or settings.DANBOORU_API_KEY
        self.user_agent = user_agent or settings.DANBOORU_USER_AGENT

        self.session = self._build_session()
        self.limiter = RateLimiter(settings.CRAWLER_RPS)

    def _build_session(self) -> requests.Session:
        """构建HTTP会话"""
        session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=32,
            pool_maxsize=32,
            max_retries=0,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "application/json",
        })
        return session

    def _build_auth_params(self) -> Dict[str, str]:
        """构建认证参数"""
        params: Dict[str, str] = {}
        if self.login and self.api_key:
            params["login"] = str(self.login)
            params["api_key"] = str(self.api_key)
        return params

    def _request(
        self,
        path: str,
        params: Dict[str, Any],
        timeout: Tuple[int, int] = (10, 60),
        max_retries: int = 6,
    ) -> Any:
        """发送API请求"""
        url = f"{self.base_url}{path}"
        merged = dict(self._build_auth_params())
        merged.update(params)

        last_exc: Optional[BaseException] = None
        backoff = 1.5

        for attempt in range(1, max_retries + 1):
            try:
                self.limiter.wait()
                resp = self.session.get(url, params=merged, timeout=timeout)

                if resp.status_code == 429:
                    sleep_s = backoff ** attempt
                    print(f"[WARN] 429 Too Many Requests, {sleep_s:.1f}s 后重试")
                    time.sleep(sleep_s)
                    continue

                if 500 <= resp.status_code < 600:
                    sleep_s = backoff ** attempt
                    print(f"[WARN] HTTP {resp.status_code}, {sleep_s:.1f}s 后重试")
                    time.sleep(sleep_s)
                    continue

                resp.raise_for_status()
                return resp.json()

            except Exception as exc:
                last_exc = exc
                if attempt == max_retries:
                    break
                sleep_s = backoff ** attempt
                print(f"[WARN] 请求失败，第 {attempt}/{max_retries} 次，{sleep_s:.1f}s 后继续: {exc}")
                time.sleep(sleep_s)

        raise RuntimeError(f"API请求最终失败: {url} err={last_exc}")

    def get_posts(
        self,
        limit: int = 100,
        page: Optional[str] = None,
        tags: Optional[List[str]] = None,
        start_id: Optional[int] = None,
        end_id: Optional[int] = None,
    ) -> List[DanbooruPost]:
        """获取帖子列表

        Args:
            limit: 每页数量 (最大1000)
            page: 分页游标，如 "b10700000"
            tags: 筛选标签列表
            start_id: 起始ID
            end_id: 结束ID

        Returns:
            帖子列表
        """
        params: Dict[str, Any] = {"limit": min(limit, 1000)}

        if page:
            params["page"] = page

        if tags:
            params["tags"] = " ".join(tags)

        if start_id and end_id:
            params["tags"] = params.get("tags", "") + f" id:{start_id}..{end_id}"
        elif start_id:
            params["tags"] = params.get("tags", "") + f" id:{start_id}.."
        elif end_id:
            params["tags"] = params.get("tags", "") + f" id:..{end_id}"

        params["tags"] = params.get("tags", "").strip()

        data = self._request("/posts.json", params)
        if not isinstance(data, list):
            return []

        return [DanbooruPost.from_dict(p) for p in data if isinstance(p, dict)]

    def get_post(self, post_id: int) -> Optional[DanbooruPost]:
        """按帖子 ID 获取单条元数据。"""
        try:
            data = self._request(f"/posts/{int(post_id)}.json", {})
        except Exception:
            return None
        if not isinstance(data, dict) or not data.get("id"):
            return None
        return DanbooruPost.from_dict(data)

    def get_first_post_for_tag(self, tag: str) -> Optional[DanbooruPost]:
        """获取某 tag 最早出现的帖子。"""
        posts = self.get_posts(limit=1, tags=[tag, "order:id"])
        return posts[0] if posts else None

    def get_tags(
        self,
        limit: int = 100,
        page: int = 1,
        category: Optional[str] = None,
        name_matches: Optional[str] = None,
        order: str = "count",
    ) -> List[DanbooruTag]:
        """获取标签列表

        Args:
            limit: 每页数量
            page: 页码
            category: 标签类别 (character, copyright, artist, general, meta, style)
            name_matches: 标签名匹配
            order: 排序方式

        Returns:
            标签列表
        """
        category_map = {
            "general": 0,
            "artist": 1,
            "copyright": 3,
            "character": 4,
            "meta": 5,
            "style": 6,
        }
        params: Dict[str, Any] = {
            "limit": min(limit, 1000),
            "page": page,
            "search[order]": order,
        }

        if category:
            params["search[category]"] = category_map.get(category, category)

        if name_matches:
            params["search[name_matches]"] = name_matches

        data = self._request("/tags.json", params)
        if not isinstance(data, list):
            return []

        return [DanbooruTag.from_dict(t) for t in data if isinstance(t, dict)]

    def get_tag_aliases(
        self,
        limit: int = 100,
        page: int = 1,
        status: Optional[str] = "active",
        order: str = "name",
    ) -> List[DanbooruTagAlias]:
        """获取 Danbooru tag aliases。"""
        params: Dict[str, Any] = {
            "limit": min(limit, 1000),
            "page": page,
            "search[order]": order,
        }
        if status:
            params["search[status]"] = status
        data = self._request("/tag_aliases.json", params)
        if not isinstance(data, list):
            return []
        return [DanbooruTagAlias.from_dict(t) for t in data if isinstance(t, dict)]

    def get_tag_implications(
        self,
        limit: int = 100,
        page: int = 1,
        status: Optional[str] = "active",
        order: str = "name",
    ) -> List[DanbooruTagImplication]:
        """获取 Danbooru tag implications。"""
        params: Dict[str, Any] = {
            "limit": min(limit, 1000),
            "page": page,
            "search[order]": order,
        }
        if status:
            params["search[status]"] = status
        data = self._request("/tag_implications.json", params)
        if not isinstance(data, list):
            return []
        return [DanbooruTagImplication.from_dict(t) for t in data if isinstance(t, dict)]

    def get_tag_counts(self, tags: List[str]) -> Dict[str, int]:
        """批量获取标签计数

        Args:
            tags: 标签名列表

        Returns:
            {tag_name: post_count} 字典
        """
        if not tags:
            return {}

        # Danbooru支持多tag查询: tags=tag1+tag2
        params = {
            "search": {"name": "+".join(tags)},
            "limit": 1000,
        }

        try:
            data = self._request("/tags.json", params)
            if not isinstance(data, list):
                return {}

            result = {}
            for tag in data:
                if isinstance(tag, dict):
                    result[tag["name"]] = int(tag.get("post_count") or 0)
            return result
        except Exception:
            return {}

    def get_post_count(self, tags: Optional[List[str]] = None) -> int:
        """获取帖子总数"""
        params: Dict[str, Any] = {}
        if tags:
            params["tags"] = " ".join(tags)

        data = self._request("/counts/posts.json", params)
        if isinstance(data, dict):
            return int(data.get("counts", {}).get("posts", 0))
        return 0

    def search_character_tags(
        self,
        min_post_count: int = 50,
        limit: int = 1000,
    ) -> List[DanbooruTag]:
        """搜索角色标签

        Args:
            min_post_count: 最低帖子数量
            limit: 返回数量

        Returns:
            角色标签列表
        """
        all_tags: List[DanbooruTag] = []
        page = 1

        while len(all_tags) < limit:
            tags = self.get_tags(
                limit=min(1000, max(1, limit - len(all_tags))),
                page=page,
                category="character",
                order="count",
            )
            if not tags:
                break

            for tag in tags:
                if tag.post_count >= min_post_count:
                    all_tags.append(tag)

                if len(all_tags) >= limit:
                    break

            page += 1

        return all_tags[:limit]


# 客户端单例
_client: Optional[DanbooruClient] = None
_client_lock = threading.Lock()


def get_danbooru_client() -> DanbooruClient:
    """获取Danbooru客户端单例"""
    global _client
    if _client is None:
        with _client_lock:
            if _client is None:
                _client = DanbooruClient()
    return _client


def reset_danbooru_client() -> None:
    """重置客户端（用于测试或��置变更）"""
    global _client
    with _client_lock:
        _client = None
