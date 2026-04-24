"""Pydantic 请求/响应模型"""

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field, ConfigDict


# ========== 项目 Schema ==========
class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None


class ProjectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime


# ========== 任务 Schema ==========
class TaskCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    task_type: str = Field(..., pattern="^(posts|tags|characters)$")
    project_id: Optional[int] = None
    # ========== 帖子爬取参数 ==========
    danbooru_ids: Optional[List[int]] = Field(default=None, description="指定帖子ID列表")
    tag_filter: Optional[str] = Field(default=None, description="Danbooru搜索语法，如 score:>20")
    limit: int = Field(default=100, ge=1, le=1000, description="最多爬取数量")
    start_id: Optional[int] = Field(default=None, description="起始ID（范围模式）")
    end_id: Optional[int] = Field(default=None, description="结束ID（范围模式）")
    # ========== 通用参数 ==========
    params: dict = Field(default_factory=dict)


class TaskUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    params: Optional[dict] = None


class TaskResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    project_id: Optional[int] = None
    name: str
    task_type: str
    params: dict
    status: str
    progress: float
    processed_count: int
    total_count: int
    error_count: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: datetime


class TaskDetailResponse(TaskResponse):
    logs: List["TaskLogResponse"] = Field(default_factory=list)
    post_count: int = 0


# ========== 任务日志 Schema ==========
class TaskLogResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: int
    level: str
    message: str
    created_at: datetime


# ========== 帖子 Schema ==========
class PostResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    task_id: Optional[int] = None
    md5: Optional[str] = None
    file_url: Optional[str] = None
    preview_url: Optional[str] = None
    sample_url: Optional[str] = None
    source: Optional[str] = None
    uploader_id: Optional[int] = None
    uploader_name: Optional[str] = None
    tag_string: Optional[str] = None
    tag_count: int
    file_ext: Optional[str] = None
    file_size: Optional[int] = None
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    score: int
    fav_count: int = 0
    rating: Optional[str] = None
    sources: Optional[List[str]] = None
    has_children: bool
    is_deleted: bool
    is_flagged: bool
    created_at: Optional[datetime] = None
    fetched_at: datetime
    file_path: Optional[str] = None
    file_verified: bool


class PostListResponse(BaseModel):
    items: List[PostResponse]
    total: int
    page: int
    page_size: int
    has_more: bool


# ========== 标签 Schema ==========
class TagResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    category: str
    post_count: int
    updated_at: datetime


class TagListResponse(BaseModel):
    items: List[TagResponse]
    total: int


# ========== 角色 Schema ==========
class CharacterResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    tag_id: int
    character_tag: str
    total_post_count: int
    recent_post_count: int
    popularity_score: float
    copyrights: List[str] = Field(default_factory=list)
    first_seen_post_id: Optional[int] = None
    first_seen_at: Optional[datetime] = None
    character_age_days: Optional[int] = None
    recent_ratio: float = 0.0
    growth_score: float = 0.0
    birth_confidence: float = 0.0
    lifecycle_notes: Optional[str] = None
    stat_at: datetime
    updated_at: datetime


class CharacterListResponse(BaseModel):
    items: List[CharacterResponse]
    total: int


class CharacterExportResponse(BaseModel):
    characters: List[dict]
    generated_at: datetime
    total_count: int
    filters: dict


# ========== 训练数据集导出 Schema ==========
class DatasetExportRequest(BaseModel):
    character_tag: str = Field(..., min_length=1, description="角色标签，如 hatsune_miku")
    limit: int = Field(default=50, ge=1, le=1000)
    min_score: Optional[int] = Field(default=None, ge=0)
    rating: Optional[str] = Field(default=None, pattern="^[gsqe]$")
    include_artist: bool = True
    download_images: bool = True
    clean_target_dir: bool = True


class DatasetExportResponse(BaseModel):
    character_tag: str
    exported_count: int
    dataset_dir: str
    errors: List[dict] = Field(default_factory=list)


# ========== 统计 Schema ==========
class DashboardStats(BaseModel):
    total_posts: int
    total_tasks: int
    running_tasks: int
    completed_tasks: int
    failed_tasks: int
    pending_tasks: int
    total_download_bytes: int
    recent_activity: List[dict] = Field(default_factory=list)


class TaskStats(BaseModel):
    total_count: int
    pending: int
    running: int
    completed: int
    failed: int
    cancelled: int


# ========== 健康检查 ==========
class HealthResponse(BaseModel):
    status: str
    version: str
    database: str
    uptime: float


# ========== 通用响应 ==========
class MessageResponse(BaseModel):
    message: str
    success: bool = True


class PaginatedResponse(BaseModel):
    items: List[Any]
    total: int
    page: int
    page_size: int
    has_more: bool


# ========== 动态引用 ==========
TaskDetailResponse.model_rebuild()
