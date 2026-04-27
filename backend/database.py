"""数据库连接与Session管理"""

from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from config import get_settings


class Base(DeclarativeBase):
    """ORM基类"""
    pass


# 数据库引擎
_engine = None
_session_factory: Optional[async_sessionmaker[AsyncSession]] = None


def get_engine():
    """获取数据库引擎（同步版本，用于alembic迁移等）"""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.DATABASE_ECHO,
        )
    return _engine


async def get_async_engine():
    """获取异步数据库引擎"""
    global _engine
    if _engine is None:
        settings = get_settings()
        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=settings.DATABASE_ECHO,
        )
    return _engine


async def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """获取Session工厂"""
    global _session_factory
    if _session_factory is None:
        engine = await get_async_engine()
        _session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库Session（依赖注入）"""
    factory = await get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """初始化数据库表"""
    import models  # noqa: F401  # Ensure ORM models are registered on Base metadata.
    engine = await get_async_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    """关闭数据库连接"""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
    _session_factory = None
