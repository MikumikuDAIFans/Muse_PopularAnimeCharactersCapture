"""依赖注入模块"""

from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db_session


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """获取数据库Session的快捷方式"""
    async for session in get_db_session():
        yield session