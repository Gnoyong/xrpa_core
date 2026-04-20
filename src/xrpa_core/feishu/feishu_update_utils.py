from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from xrpa_core.core.logger import logger
from xrpa_core.feishu.feishu_sheet import FeishuSheet
from xrpa_core.x_framework.x_excel import number_to_letters


def build_column_values(
    row_updates: dict[int, dict[str, Any]],
    key: str,
    min_row: int | None = None,
    max_row: int | None = None,
    value_map: Mapping[Any, Any] | None = None,
) -> tuple[list, int, int]:
    if not row_updates:
        return [], 0, 0

    min_row = min_row if min_row is not None else min(row_updates.keys())
    max_row = max_row if max_row is not None else max(row_updates.keys())

    values = []
    for row in range(min_row, max_row + 1):
        if row in row_updates:
            value = row_updates[row].get(key)
            if value_map is not None:
                value = value_map.get(value, value)
            values.append([value])
        else:
            values.append(None)

    return values, min_row, max_row


def write_column_range(
    sheet: FeishuSheet,
    col_idx: int,
    values: list,
    min_row: int,
    max_row: int,
    desc: str,
) -> None:
    if not values or min_row <= 0 or max_row <= 0:
        logger.info(f"[{desc}] 列没有更新数据")
        return

    col_letter = number_to_letters(col_idx + 1)
    cell_range = f"{col_letter}{min_row}:{col_letter}{max_row}"
    logger.info(f"批量更新 [{desc}] 列，范围: {cell_range}")
    sheet.write_range(cell_range, values)


def write_columns_from_updates(
    sheet: FeishuSheet,
    column_map: Mapping[str, int],
    row_updates: dict[int, dict[str, Any]],
    value_maps: Mapping[str, Mapping[Any, Any]] | None = None,
    desc_map: Mapping[str, str] | None = None,
) -> None:
    if not row_updates:
        logger.warning("没有需要更新的数据")
        return

    min_row = min(row_updates.keys())
    max_row = max(row_updates.keys())

    for key, col_idx in column_map.items():
        desc = desc_map.get(key, key) if desc_map else key
        value_map = value_maps.get(key) if value_maps else None
        values, _, _ = build_column_values(
            row_updates,
            key,
            min_row=min_row,
            max_row=max_row,
            value_map=value_map,
        )
        write_column_range(sheet, col_idx, values, min_row, max_row, desc)
