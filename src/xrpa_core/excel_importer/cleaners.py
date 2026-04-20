"""
常用字段清洗器集合

所有清洗器均为 Callable[[Any], Any]，可直接用于 ExcelImporterConfig.field_cleaners。
"""

import re
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from xrpa_core.excel_importer.importer import SkipRowError


class Cleaners:
    """提供常用字段清洗器的工具类，所有方法均为静态方法，返回清洗函数。"""

    # ------------------------------------------------------------------ #
    #  字符串                                                               #
    # ------------------------------------------------------------------ #

    @staticmethod
    def strip(chars: str | None = None) -> Callable[[Any], Any]:
        """去除首尾空白（或指定字符）。None值透传。"""

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return str(value).strip(chars)

        return _clean

    @staticmethod
    def strip_required(chars: str | None = None) -> Callable[[Any], Any]:
        """去除首尾空白后若为空则跳过该行。"""

        def _clean(value: Any) -> Any:
            if value is None:
                raise SkipRowError
            stripped = str(value).strip(chars)
            if not stripped:
                raise SkipRowError
            return stripped

        return _clean

    @staticmethod
    def upper() -> Callable[[Any], Any]:
        """转换为大写。None值透传。"""

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return str(value).strip().upper()

        return _clean

    @staticmethod
    def lower() -> Callable[[Any], Any]:
        """转换为小写。None值透传。"""

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return str(value).strip().lower()

        return _clean

    @staticmethod
    def replace(old: str, new: str = "") -> Callable[[Any], Any]:
        """替换字符串中的指定子串。None值透传。"""

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return str(value).replace(old, new)

        return _clean

    @staticmethod
    def regex_replace(
        pattern: str, repl: str = "", flags: int = 0
    ) -> Callable[[Any], Any]:
        """正则替换。None值透传。"""
        compiled = re.compile(pattern, flags)

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return compiled.sub(repl, str(value))

        return _clean

    @staticmethod
    def max_length(n: int) -> Callable[[Any], Any]:
        """截断字符串至最大长度 n。None值透传。"""

        def _clean(value: Any) -> Any:
            if value is None:
                return None
            return str(value)[:n]

        return _clean

    @staticmethod
    def default(default_value: Any) -> Callable[[Any], Any]:
        """None时返回默认值。"""

        def _clean(value: Any) -> Any:
            return default_value if value is None else value

        return _clean

    @staticmethod
    def skip_if_none() -> Callable[[Any], Any]:
        """None时跳过该行。"""

        def _clean(value: Any) -> Any:
            if value is None:
                raise SkipRowError
            return value

        return _clean

    @staticmethod
    def skip_if_empty() -> Callable[[Any], Any]:
        """None或空字符串时跳过该行。"""

        def _clean(value: Any) -> Any:
            if value is None or str(value).strip() == "":
                raise SkipRowError
            return value

        return _clean

    # ------------------------------------------------------------------ #
    #  数值                                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_int(default: int | None = None) -> Callable[[Any], Any]:
        """转换为整数。转换失败时返回 default，若 default 为 None 则透传 None。"""

        def _clean(value: Any) -> int | None:
            if value is None:
                return default
            try:
                text = str(value).strip()
                # 支持浮点字符串如 "3.0"
                return int(float(text))
            except (ValueError, TypeError):
                return default

        return _clean

    @staticmethod
    def to_float(default: float | None = None) -> Callable[[Any], Any]:
        """转换为浮点数。转换失败时返回 default。"""

        def _clean(value: Any) -> float | None:
            if value is None:
                return default
            try:
                return float(str(value).strip())
            except (ValueError, TypeError):
                return default

        return _clean

    @staticmethod
    def to_decimal(
        places: int | None = None, default: Decimal | None = None
    ) -> Callable[[Any], Any]:
        """转换为 Decimal，可选保留小数位。转换失败时返回 default。"""

        def _clean(value: Any) -> Decimal | None:
            if value is None:
                return default
            try:
                d = Decimal(str(value).strip())
                if places is not None:
                    d = round(d, places)
                return d
            except InvalidOperation:
                return default

        return _clean

    # ------------------------------------------------------------------ #
    #  日期 / 时间                                                          #
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_date(
        fmt: str = "%Y-%m-%d", default: date | None = None
    ) -> Callable[[Any], Any]:
        """按指定格式解析为 date。解析失败时返回 default。"""

        def _clean(value: Any) -> date | None:
            if value is None:
                return default
            if isinstance(value, date):
                return value
            try:
                return datetime.strptime(str(value).strip(), fmt).date()
            except (ValueError, TypeError):
                return default

        return _clean

    @staticmethod
    def to_datetime(
        fmt: str = "%Y-%m-%d %H:%M:%S", default: datetime | None = None
    ) -> Callable[[Any], Any]:
        """按指定格式解析为 datetime。解析失败时返回 default。"""

        def _clean(value: Any) -> datetime | None:
            if value is None:
                return default
            if isinstance(value, datetime):
                return value

            return datetime.strptime(str(value).strip(), fmt)

        return _clean

    # ------------------------------------------------------------------ #
    #  布尔                                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_bool(
        true_values: tuple[str, ...] = ("1", "true", "yes", "是", "y"),
        false_values: tuple[str, ...] = ("0", "false", "no", "否", "n"),
        default: bool | None = None,
    ) -> Callable[[Any], Any]:
        """将字符串解析为布尔值（大小写不敏感）。无法匹配时返回 default。"""

        def _clean(value: Any) -> bool | None:
            if value is None:
                return default
            text = str(value).strip().lower()
            if text in true_values:
                return True
            if text in false_values:
                return False
            return default

        return _clean

    # ------------------------------------------------------------------ #
    #  组合                                                                 #
    # ------------------------------------------------------------------ #

    @staticmethod
    def chain(*cleaners: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """将多个清洗器串联，按顺序依次应用。"""

        def _clean(value: Any) -> Any:
            for cleaner in cleaners:
                value = cleaner(value)
            return value

        return _clean
