"""SQLAlchemy ORM 模型"""

from datetime import datetime
from typing import Optional

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), default=None)
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), default=None
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
    stat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow
    )

    # 关联
    task: Mapped["Task"] = relationship("Task", back_populates="logs")

    __table_args__ = (
        Index("ix_task_log_task_id", "task_id"),
        Index("ix_task_log_level", "level"),
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
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
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
        DateTime(timezone=True), default=datetime.utcnow
    )

    __table_args__ = (
        Index("ix_download_log_post_id", "post_id"),
        Index("ix_download_log_character", "character_tag"),
        Index("ix_download_log_status", "status"),
    )
