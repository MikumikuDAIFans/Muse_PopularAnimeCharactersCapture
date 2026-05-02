"""SQLAlchemy ORM 模型"""

from datetime import UTC, date, datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from database import Base


# ========== 项目表 ==========
class Project(Base):
    """项目表"""
    __tablename__ = "project"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # 关联
    tasks: Mapped[list["Task"]] = relationship(
        "Task", back_populates="project", cascade="all, delete-orphan"
    )


# ========== 任务表 ==========
class Task(Base):
    """爬取任务表"""
    __tablename__ = "task"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    project_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("project.id"), default=None
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_type: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "task_type IN ('posts', 'tags', 'characters')",
            name="ck_task_type",
        ),
        nullable=False,
    )

    # 任务参数 (JSON存储)
    params: Mapped[dict] = mapped_column(JSON, default=dict)

    # 执行状态
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "status IN ('pending','running','paused','completed','failed','cancelled')",
            name="ck_task_status",
        ),
        default="pending",
    )

    # 进度
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    processed_count: Mapped[int] = mapped_column(Integer, default=0)
    total_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)

    # 时间戳
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), default=None
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # 关联
    project: Mapped[Optional["Project"]] = relationship("Project", back_populates="tasks")
    logs: Mapped[list["TaskLog"]] = relationship(
        "TaskLog", back_populates="task", cascade="all, delete-orphan"
    )
    posts: Mapped[list["Post"]] = relationship("Post", back_populates="task")

    __table_args__ = (
        Index("ix_task_status", "status"),
        Index("ix_task_project_id", "project_id"),
    )


# ========== 帖子元数据表 ==========
class Post(Base):
    """帖子元数据表"""
    __tablename__ = "post"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    task_id: Mapped[Optional[int]] = mapped_column(ForeignKey("task.id"), default=None)

    # Danbooru核心字段
    md5: Mapped[Optional[str]] = mapped_column(String(32), default=None)
    file_url: Mapped[Optional[str]] = mapped_column(Text, default=None)
    preview_url: Mapped[Optional[str]] = mapped_column(Text, default=None)
    sample_url: Mapped[Optional[str]] = mapped_column(Text, default=None)
    source: Mapped[Optional[str]] = mapped_column(Text, default=None)
    uploader_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    uploader_name: Mapped[Optional[str]] = mapped_column(String(255), default=None)
    tag_string: Mapped[Optional[str]] = mapped_column(Text, default=None)
    tag_count: Mapped[int] = mapped_column(Integer, default=0)
    file_ext: Mapped[Optional[str]] = mapped_column(String(16), default=None)
    file_size: Mapped[Optional[int]] = mapped_column(BigInteger, default=None)
    image_width: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    image_height: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    score: Mapped[int] = mapped_column(Integer, default=0)
    fav_count: Mapped[int] = mapped_column(Integer, default=0)
    rating: Mapped[Optional[str]] = mapped_column(String(16), default=None)
    sources: Mapped[Optional[str]] = mapped_column(JSON, default=None)

    # 元数据
    has_children: Mapped[bool] = mapped_column(Boolean, default=False)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    is_flagged: Mapped[bool] = mapped_column(Boolean, default=False)

    # 时间戳
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    # 文件路径
    file_path: Mapped[Optional[str]] = mapped_column(Text, default=None)
    file_verified: Mapped[bool] = mapped_column(Boolean, default=False)

    # 关联
    task: Mapped[Optional["Task"]] = relationship("Task", back_populates="posts")
    tags: Mapped[list["PostTag"]] = relationship(
        "PostTag", back_populates="post", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_post_task_id", "task_id"),
        Index("ix_post_fetched_at", "fetched_at"),
        Index("ix_post_md5", "md5"),
        Index("ix_post_created_at", "created_at"),
    )


# ========== 标签表 ==========
class Tag(Base):
    """标签表"""
    __tablename__ = "tag"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    category: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "category IN ('character','copyright','artist','general','meta','style')",
            name="ck_tag_category",
        ),
        nullable=False,
    )
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # 关联
    characters: Mapped[list["Character"]] = relationship(
        "Character", back_populates="tag", uselist=False
    )
    copyrights: Mapped[list["Copyright"]] = relationship(
        "Copyright", back_populates="tag", uselist=False
    )

    __table_args__ = (
        Index("ix_tag_category", "category"),
        Index("ix_tag_name", "name"),
    )


class TagAlias(Base):
    """Danbooru 标签别名表。"""
    __tablename__ = "tag_alias"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    antecedent_name: Mapped[str] = mapped_column(String(512), nullable=False)
    consequent_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("ix_tag_alias_antecedent", "antecedent_name"),
        Index("ix_tag_alias_consequent", "consequent_name"),
    )


class TagImplication(Base):
    """Danbooru 标签蕴含表。"""
    __tablename__ = "tag_implication"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    antecedent_name: Mapped[str] = mapped_column(String(512), nullable=False)
    consequent_name: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), default="active")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("ix_tag_implication_antecedent", "antecedent_name"),
        Index("ix_tag_implication_consequent", "consequent_name"),
    )


class SyncCheckpoint(Base):
    """长任务同步检查点。"""
    __tablename__ = "sync_checkpoint"

    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    checkpoint: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


# ========== 帖子-标签关联表 ==========
class PostTag(Base):
    """帖子-标签关联表"""
    __tablename__ = "post_tag"

    post_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("post.id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), primary_key=True)

    # 关联
    post: Mapped["Post"] = relationship("Post", back_populates="tags")
    tag: Mapped["Tag"] = relationship("Tag")

    __table_args__ = (
        Index("ix_post_tag_tag_id_post_id", "tag_id", "post_id"),
    )


# ========== 角色表 ==========
class Character(Base):
    """角色表"""
    __tablename__ = "character"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), unique=True)

    # 热度相关
    total_post_count: Mapped[int] = mapped_column(Integer, default=0)
    recent_post_count: Mapped[int] = mapped_column(Integer, default=0)
    popularity_score: Mapped[float] = mapped_column(Float, default=0.0)
    first_seen_post_id: Mapped[Optional[int]] = mapped_column(BigInteger, default=None)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    character_age_days: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    recent_ratio: Mapped[float] = mapped_column(Float, default=0.0)
    growth_score: Mapped[float] = mapped_column(Float, default=0.0)
    birth_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    lifecycle_notes: Mapped[Optional[str]] = mapped_column(Text, default=None)

    # 统计元数据
    stat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # 关联
    tag: Mapped["Tag"] = relationship("Tag", back_populates="characters")
    copyrights: Mapped[list["CharacterCopyright"]] = relationship(
        "CharacterCopyright", back_populates="character"
    )

    __table_args__ = (
        Index("ix_character_score", "popularity_score"),
        Index("ix_character_total", "total_post_count"),
    )


# ========== 作品表 ==========
class Copyright(Base):
    """作品表"""
    __tablename__ = "copyright"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), unique=True)
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    # 关联
    tag: Mapped["Tag"] = relationship("Tag", back_populates="copyrights")
    characters: Mapped[list["CharacterCopyright"]] = relationship(
        "CharacterCopyright", back_populates="copyright"
    )


# ========== 角色-作品关联表 ==========
class CharacterCopyright(Base):
    """角色-作品关联表"""
    __tablename__ = "character_copyright"

    character_tag_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("character.tag_id"), primary_key=True
    )
    copyright_tag_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("copyright.tag_id"), primary_key=True
    )

    # 关联
    character: Mapped["Character"] = relationship("Character", back_populates="copyrights")
    copyright: Mapped["Copyright"] = relationship("Copyright", back_populates="characters")


# ========== 任务日志表 ==========
class TaskLog(Base):
    """任务日志表"""
    __tablename__ = "task_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[int] = mapped_column(ForeignKey("task.id"), nullable=False)
    level: Mapped[str] = mapped_column(
        String(16),
        CheckConstraint("level IN ('INFO','WARN','ERROR')", name="ck_log_level"),
        nullable=False,
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    # 关联
    task: Mapped["Task"] = relationship("Task", back_populates="logs")

    __table_args__ = (
        Index("ix_task_log_task_id", "task_id"),
        Index("ix_task_log_level", "level"),
    )


class SyncJob(Base):
    """正式同步任务状态表。"""
    __tablename__ = "sync_job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    job_type: Mapped[str] = mapped_column(String(64), nullable=False, default="metadata")
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','cancelled')",
            name="ck_sync_job_status",
        ),
        default="pending",
    )
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    total_shards: Mapped[int] = mapped_column(Integer, default=0)
    completed_shards: Mapped[int] = mapped_column(Integer, default=0)
    failed_shards: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    shards: Mapped[list["SyncShard"]] = relationship(
        "SyncShard", back_populates="job", cascade="all, delete-orphan"
    )
    logs: Mapped[list["JobLog"]] = relationship(
        "JobLog", back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_sync_job_status", "status"),
        Index("ix_sync_job_type", "job_type"),
    )


class SyncShard(Base):
    """可恢复的同步分片状态表。"""
    __tablename__ = "sync_shard"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("sync_job.id"), nullable=False)
    task_id: Mapped[Optional[int]] = mapped_column(Integer, default=None)
    shard_key: Mapped[str] = mapped_column(String(255), nullable=False)
    tag_filter: Mapped[Optional[str]] = mapped_column(Text, default=None)
    output_path: Mapped[Optional[str]] = mapped_column(Text, default=None)
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','skipped')",
            name="ck_sync_shard_status",
        ),
        default="pending",
    )
    line_count: Mapped[int] = mapped_column(Integer, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, default=0)
    invalid_count: Mapped[int] = mapped_column(Integer, default=0)
    error_count: Mapped[int] = mapped_column(Integer, default=0)
    checkpoint: Mapped[dict] = mapped_column(JSON, default=dict)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    job: Mapped["SyncJob"] = relationship("SyncJob", back_populates="shards")

    __table_args__ = (
        UniqueConstraint("job_id", "shard_key", name="uq_sync_shard_job_key"),
        Index("ix_sync_shard_status", "status"),
        Index("ix_sync_shard_task_id", "task_id"),
    )


class JobLog(Base):
    """正式任务运行日志。"""
    __tablename__ = "job_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sync_job.id"), default=None)
    shard_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sync_shard.id"), default=None)
    level: Mapped[str] = mapped_column(
        String(16),
        CheckConstraint("level IN ('DEBUG','INFO','WARN','ERROR')", name="ck_job_log_level"),
        nullable=False,
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    job: Mapped[Optional["SyncJob"]] = relationship("SyncJob", back_populates="logs")

    __table_args__ = (
        Index("ix_job_log_job_id", "job_id"),
        Index("ix_job_log_shard_id", "shard_id"),
        Index("ix_job_log_level", "level"),
    )


class CharacterMonthlyStats(Base):
    """角色月度聚合表。"""
    __tablename__ = "character_monthly_stats"

    character_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), primary_key=True)
    month_start: Mapped[date] = mapped_column(Date, primary_key=True)
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    fav_count_sum: Mapped[int] = mapped_column(Integer, default=0)
    score_sum: Mapped[int] = mapped_column(Integer, default=0)
    first_post_id: Mapped[Optional[int]] = mapped_column(BigInteger, default=None)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("ix_character_monthly_stats_month", "month_start"),
        Index("ix_character_monthly_stats_count", "post_count"),
    )


class CharacterMonthlyCopyright(Base):
    """角色-作品月度共现聚合表。"""
    __tablename__ = "character_monthly_copyright"

    character_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), primary_key=True)
    copyright_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), primary_key=True)
    month_start: Mapped[date] = mapped_column(Date, primary_key=True)
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("ix_character_monthly_copyright_month", "month_start"),
        Index("ix_character_monthly_copyright_count", "post_count"),
    )


class CharacterBirthCache(Base):
    """角色首现缓存表。"""
    __tablename__ = "character_birth_cache"

    character_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), primary_key=True)
    first_seen_post_id: Mapped[Optional[int]] = mapped_column(BigInteger, default=None)
    first_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    birth_confidence: Mapped[float] = mapped_column(Float, default=0.0)
    source: Mapped[str] = mapped_column(String(64), default="post_history")
    notes: Mapped[Optional[str]] = mapped_column(Text, default=None)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class RankingSnapshot(Base):
    """榜单快照表。"""
    __tablename__ = "ranking_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ranking_type: Mapped[str] = mapped_column(String(64), nullable=False)
    window_months: Mapped[int] = mapped_column(Integer, default=6)
    top_n: Mapped[int] = mapped_column(Integer, default=200)
    min_post_count: Mapped[int] = mapped_column(Integer, default=50)
    filters: Mapped[dict] = mapped_column(JSON, default=dict)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    export_json_path: Mapped[Optional[str]] = mapped_column(Text, default=None)
    export_csv_path: Mapped[Optional[str]] = mapped_column(Text, default=None)

    items: Mapped[list["RankingSnapshotItem"]] = relationship(
        "RankingSnapshotItem", back_populates="snapshot", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_ranking_snapshot_type_generated", "ranking_type", "generated_at"),
    )


class RankingSnapshotItem(Base):
    """榜单快照条目表。"""
    __tablename__ = "ranking_snapshot_item"

    snapshot_id: Mapped[int] = mapped_column(ForeignKey("ranking_snapshot.id"), primary_key=True)
    rank: Mapped[int] = mapped_column(Integer, primary_key=True)
    character_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), nullable=False)
    character_tag: Mapped[str] = mapped_column(String(512), nullable=False)
    post_count: Mapped[int] = mapped_column(Integer, default=0)
    recent_post_count: Mapped[int] = mapped_column(Integer, default=0)
    popularity_score: Mapped[float] = mapped_column(Float, default=0.0)
    growth_score: Mapped[float] = mapped_column(Float, default=0.0)
    payload: Mapped[dict] = mapped_column(JSON, default=dict)

    snapshot: Mapped["RankingSnapshot"] = relationship("RankingSnapshot", back_populates="items")

    __table_args__ = (
        Index("ix_ranking_snapshot_item_character", "character_tag_id"),
    )


class DownloadJob(Base):
    """榜单驱动的下载任务。"""
    __tablename__ = "download_job"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_snapshot_id: Mapped[Optional[int]] = mapped_column(ForeignKey("ranking_snapshot.id"), default=None)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','cancelled')",
            name="ck_download_job_status",
        ),
        default="pending",
    )
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    items: Mapped[list["DownloadJobItem"]] = relationship(
        "DownloadJobItem", back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_download_job_status", "status"),
    )


class DownloadJobItem(Base):
    """榜单角色定向下载条目。"""
    __tablename__ = "download_job_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_id: Mapped[int] = mapped_column(ForeignKey("download_job.id"), nullable=False)
    character_tag_id: Mapped[int] = mapped_column(Integer, ForeignKey("tag.id"), nullable=False)
    character_tag: Mapped[str] = mapped_column(String(512), nullable=False)
    target_count: Mapped[int] = mapped_column(Integer, default=0)
    downloaded_count: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint(
            "status IN ('pending','running','completed','failed','skipped')",
            name="ck_download_job_item_status",
        ),
        default="pending",
    )
    output_dir: Mapped[Optional[str]] = mapped_column(Text, default=None)
    error_message: Mapped[Optional[str]] = mapped_column(Text, default=None)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )

    job: Mapped["DownloadJob"] = relationship("DownloadJob", back_populates="items")

    __table_args__ = (
        UniqueConstraint("job_id", "character_tag_id", name="uq_download_job_item_character"),
        Index("ix_download_job_item_status", "status"),
    )


# ========== 系统统计表 ==========
class SystemStats(Base):
    """系统统计表"""
    __tablename__ = "system_stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    total_posts: Mapped[int] = mapped_column(Integer, default=0)
    total_tasks: Mapped[int] = mapped_column(Integer, default=0)
    completed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    failed_tasks: Mapped[int] = mapped_column(Integer, default=0)
    total_download_bytes: Mapped[int] = mapped_column(BigInteger, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC)
    )


class DownloadLog(Base):
    """数据集下载/导出日志。"""
    __tablename__ = "download_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    post_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    character_tag: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32),
        CheckConstraint("status IN ('success','skipped','failed')", name="ck_download_status"),
        nullable=False,
    )
    stage: Mapped[str] = mapped_column(String(64), default="download")
    file_path: Mapped[Optional[str]] = mapped_column(Text, default=None)
    error_message: Mapped[Optional[str]] = mapped_column(Text, default=None)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(UTC)
    )

    __table_args__ = (
        Index("ix_download_log_post_id", "post_id"),
        Index("ix_download_log_character", "character_tag"),
        Index("ix_download_log_status", "status"),
    )
