from datetime import datetime
from typing import Any

from sqlalchemy import (
    String,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.sqltypes import JSON, DateTime

from xrpa_core.config import app_config


# ==========================================================
# 键值对
# ==========================================================
class Base(DeclarativeBase):
    pass


class KeyValue(Base):
    __tablename__ = "kv"

    key: Mapped[str] = mapped_column(String(128), primary_key=True, comment="键")

    value: Mapped[Any] = mapped_column(JSON, nullable=False, comment="值（JSON）")

    description: Mapped[str | None] = mapped_column(
        String(255), nullable=True, comment="描述"
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="更新时间",
    )

# 数据库管理类
class DatabaseManager:
    def __init__(self, db_url: str | None = None):
        """初始化数据库连接"""
        if db_url is None:
            db_url = app_config.db_url

        self.engine = create_engine(db_url, echo=False)

    def get_session(self) -> Session:
        """获取数据库会话"""
        return Session(self.engine)


db_manager = DatabaseManager()
