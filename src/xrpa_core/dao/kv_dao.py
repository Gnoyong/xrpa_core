from typing import Any

from sqlalchemy.orm import Session

from xrpa_core.db.models import KeyValue


class KVDao:
    """简单的键值存取 DAO。"""

    def __init__(self) -> None:
        pass

    @staticmethod
    def set(
        session: Session,
        key: str,
        value: Any,
        description: str | None = None,
    ) -> KeyValue:
        obj = session.get(KeyValue, key)
        if obj is None:
            obj = KeyValue(key=key, value=value, description=description)
            session.add(obj)
        else:
            obj.value = value
            if description is not None:
                obj.description = description
        return obj

    @staticmethod
    def delete(session: Session, key: str) -> bool:
        obj = session.get(KeyValue, key)
        if obj is None:
            return False
        session.delete(obj)
        return True

    @staticmethod
    def list(
        session: Session, prefix: str | None = None, limit: int = 100
    ) -> list[KeyValue]:
        q = session.query(KeyValue)
        if prefix is not None:
            q = q.filter(KeyValue.key.like(f"{prefix}%"))
        return q.limit(limit).all()

    @staticmethod
    def get(session: Session, key: str) -> KeyValue | None:
        return session.get(KeyValue, key)


# module-level convenience instance
kv_dao = KVDao()
