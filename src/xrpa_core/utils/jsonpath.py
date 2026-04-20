from __future__ import annotations

from typing import Any

# from jsonpath_ng.parser import parse
from jsonpath_ng.ext import parse

from xrpa_core.core.logger import logger


def jsonpath_values(
    data: dict | list,
    path: str,
    *,
    default: list[Any] | None = None,
    raise_on_error: bool = False,
) -> list[Any]:
    """
    在 dict/list 中执行 JSONPath，并返回全部匹配值。

    示例:
            jsonpath_values(payload, "$.data.items[*].id")
    """
    try:
        expr = parse(path)
        return [match.value for match in expr.find(data)]
    except Exception as exc:  # pragma: no cover - 防御性分支
        if raise_on_error:
            raise
        logger.warning(f"JSONPath 解析失败 path={path} error={exc}")
        return [] if default is None else default


def jsonpath_first(
    data: dict | list,
    path: str,
    *,
    default: Any = None,
    raise_on_error: bool = False,
) -> Any:
    """在 dict/list 中执行 JSONPath，并返回首个匹配值。"""
    values = jsonpath_values(
        data,
        path,
        default=[],
        raise_on_error=raise_on_error,
    )
    return values[0] if values else default


def jsonpath_exists(
    data: dict | list,
    path: str,
    *,
    raise_on_error: bool = False,
) -> bool:
    """判断 JSONPath 是否能在 dict/list 中匹配到至少一个值。"""
    return bool(jsonpath_values(data, path, raise_on_error=raise_on_error))
