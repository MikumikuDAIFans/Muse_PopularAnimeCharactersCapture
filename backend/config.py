"""应用配置管理"""

import os
from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """应用配置"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ========== 应用基础配置 ==========
    APP_NAME: str = "Muse_DataLoad"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = False
    API_PREFIX: str = "/api"

    # ========== 数据库配置 ==========
    DATABASE_URL: str = Field(
        default="sqlite+aiosqlite:///./muse_dataload.db",
        description="SQLAlchemy数据库连接URL"
    )
    DATABASE_ECHO: bool = Field(
        default=False,
        description="是否输出SQL语句到日志"
    )

    # ========== Danbooru API 配置 ==========
    DANBOORU_BASE_URL: str = "https://danbooru.donmai.us"
    DANBOORU_LOGIN: Optional[str] = Field(default=None, description="Danbooru登录名")
    DANBOORU_API_KEY: Optional[str] = Field(default=None, description="Danbooru API Key")
    DANBOORU_USER_AGENT: str = "Muse_DataLoad/0.1 (+https://github.com/ckmuse)"

    # ========== 爬虫配置 ==========
    CRAWLER_RPS: float = Field(
        default=1.5,
        description="API请求速率限制（每秒请求数）"
    )
    CRAWLER_THREADS: int = Field(
        default=6,
        description="爬虫线程池大小"
    )
    DOWNLOAD_PROCESSES: int = Field(
        default=4,
        description="下载进程池大小"
    )
    DOWNLOAD_RPS_PER_PROCESS: float = Field(
        default=0.0,
        description="单进程下载速率限制"
    )
    API_MAX_RETRIES: int = Field(
        default=6,
        description="API最大重试次数"
    )
    API_TIMEOUT: tuple = Field(
        default=(10, 60),
        description="API超时（连接超时, 读取超时）"
    )
    DOWNLOAD_TIMEOUT: tuple = Field(
        default=(15, 300),
        description="下载超时（连接超时, 读取超时）"
    )

    # ========== 文件配置 ==========
    OUTPUT_ROOT: str = Field(
        default="./output",
        description="输出根目录"
    )
    METADATA_DIR: str = Field(
        default="metadata",
        description="元数据目录名"
    )
    IMAGE_DIR: str = Field(
        default="images",
        description="图片目录名"
    )
    USE_IMAGE_SUBDIRS: bool = Field(
        default=True,
        description="是否使用子目录分组"
    )
    IMAGE_SUBDIR_DIVISOR: int = Field(
        default=10000,
        description="子目录分组大小"
    )
    ALLOWED_FILE_EXTS: set = Field(
        default={"jpg", "jpeg", "png", "webp"},
        description="允许的图片扩展名"
    )

    # ========== 校验配置 ==========
    VERIFY_MD5: bool = Field(
        default=True,
        description="是否校验MD5"
    )
    VERIFY_FILE_SIZE: bool = Field(
        default=True,
        description="是否校验文件大小"
    )

    # ========== 服务器配置 ==========
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    CORS_ORIGINS: list = Field(
        default=["*"],
        description="CORS允许的来源"
    )


@lru_cache
def get_settings() -> Settings:
    """获取配置单例"""
    return Settings()