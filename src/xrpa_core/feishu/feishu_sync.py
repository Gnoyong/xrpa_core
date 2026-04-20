"""
feishu_exporter.py

通用飞书表格导出器。
输入：任意 SQL 查询结果（List[Dict]），+ 一个声明式字段配置 → 自动生成带样式、合并、验证的飞书 Sheet。

---------------------------------------------
快速上手示例（见底部 __main__）
---------------------------------------------
"""

import csv
import tempfile
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from time import sleep
from typing import Any

from xrpa_core.core import logger
from xrpa_core.feishu.feishu_client import get_client
from xrpa_core.feishu.feishu_doc_exporter import (
    FeishuDocExporter,
    FeishuDocType,
    FeishuFileExtension,
)
from xrpa_core.feishu.feishu_sheet import (
    AddProtectedDimensionRequestModel,
    FeishuSheet,
    FeishuSpreadSheet,
    ProtectedDimensionModel,
    UpdateSheetRequestModel,
)
from xrpa_core.feishu.feishu_update_utils import write_columns_from_updates
from xrpa_core.x_framework.x_excel import number_to_letters


# ============================================================
# 1. 字段级配置 FieldConfig  —  唯一需要用户填写的声明式入口
# ============================================================
class FieldConfig:
    """单列的全部配置，声明一次，导出器自动处理所有格式化/样式/验证。

    Attributes:
        key:            从每一行 dict 里读值的 key。None → 该列由 transform 计算。
        width:          列宽 (px)。None → 使用平台默认。
        field_type:     逻辑类型，影响样式分组：text | number | date | merged。
        validation:     下拉框验证配置（详见示例）。
        transform:      可选的值转换函数，用于DB向飞书表格的值转换 (row_dict, row_index) -> str。
                        有 transform 时 key 的值会被忽略；
                        无 transform 且 key 为 None 时返回空字符串。
        transform_to_db: 反向转换函数 (row_dict, row_index) -> str，用于 db_to_feishu 同步更新时从飞书值转换为数据库的值。
        formatter:      列数字格式（飞书 style.formatter），如 "@"、"#,##0.00" 等。
    """

    def __init__(
        self,
        header: str,
        key: str | None = None,
        width: int | None = None,
        field_type: str = "text",
        validation: dict | None = None,
        transform_to_fs: Callable[[dict[str, Any], int], str] | None = None,
        transform_to_db: Callable[[Any], str] | None = None,
        protected: bool = False,
        formatter: str | None = None,
        h_align: int = 1,
        v_align: int = 1,
        fore_color: str | None = None,
    ):
        self.key = key
        self.width = width
        self.field_type = field_type
        self.validation = validation
        self.transform = transform_to_fs
        self.transform_to_db = transform_to_db
        self.header = header
        self.protected = protected
        self.formatter = formatter
        self.h_align = h_align
        self.v_align = v_align
        self.fore_color = fore_color

    def is_real_field(self) -> bool:
        return self.key is not None


# ============================================================
# 2. 表级配置 SheetExportConfig  —  控制合并、分组、钩子等行为
# ============================================================
class SheetExportConfig:
    """一次导出的全局配置。

    Attributes:
        fields:             OrderedDict-style: {表头名: FieldConfig}，顺序即列顺序。
        group_by_key:       用于合并单元格的分组 key（对应 row dict 里的某个 key）。
                            None → 不做任何合并。
        merge_columns:      合并范围覆盖的表头列表 [start_header, ..., end_header]。
                            默认 None → 自动取 fields 里 field_type=="merged" 的列。
        frozen_row_count:   从首行开始要冻结的行数。None → 不设置冻结。
        frozen_col_count:   从首列开始要冻结的列数。None → 不设置冻结。
        on_export_success:  导出成功后的回调 (sheet_name, rows) -> None，可用于更新 DB 标记等。
        write_delay_sec:    写入数据后的等待秒数（给飞书后端同步留时间）。默认 0。
    """

    def __init__(
        self,
        fields: dict[str, FieldConfig],
        primary_header: str,
        group_by_key: str | None = None,
        merge_columns: list[str] | None = None,
        frozen_row_count: int | None = None,
        frozen_col_count: int | None = None,
        on_export_success: Callable[[str, list[dict]], None] | None = None,
        write_delay_sec: float = 0,
        update_time_header: str | None = None,
        formatter: str = "@",
    ):
        self.fields = fields
        self.group_by_key = group_by_key
        self.merge_columns = merge_columns
        self.frozen_row_count = frozen_row_count
        self.frozen_col_count = frozen_col_count
        self.on_export_success = on_export_success
        self.write_delay_sec = write_delay_sec
        self.primary_header = primary_header
        self.update_time_header = update_time_header
        self.formatter = formatter

    # -- 派生属性 ----------------------------------------------------------
    @property
    def headers(self) -> list[str]:
        return list(self.fields.keys())

    @property
    def resolved_merge_columns(self) -> list[str]:
        """如果用户没显式指定 merge_columns，自动返回 field_type=='merged' 的列。"""
        if self.merge_columns is not None:
            return self.merge_columns
        return [name for name, cfg in self.fields.items() if cfg.field_type == "merged"]


# ============================================================
# 3. 样式常量  —  可按需自定义
# ============================================================
STYLE_CONFIG: dict[str, dict] = {
    "header": {
        "font": {"bold": True},
        "textDecoration": 0,
        "hAlign": 1,
        "vAlign": 1,
        "foreColor": "#FFFFFF",
        "backColor": "#4A90E2",
        "borderType": "FULL_BORDER",
        "borderColor": "#2E5C8A",
        "clean": False,
    },
    "data": {
        "textDecoration": 0,
        # "hAlign": 1,
        # "vAlign": 1,
        "foreColor": "#333333",
        "backColor": "#FFFFFF",
        "borderType": "FULL_BORDER",
        "borderColor": "#E0E0E0",
        "clean": False,
    },
    "merged": {
        "textDecoration": 0,
        # "hAlign": 1,
        # "vAlign": 1,
        "foreColor": "#333333",
        "backColor": "#E3F2FD",
        "borderType": "FULL_BORDER",
        "borderColor": "#90CAF9",
        "clean": False,
    },
    "merged_alternate": {
        "textDecoration": 0,
        # "hAlign": 1,
        # "vAlign": 1,
        "foreColor": "#333333",
        "backColor": "#FFFFFF",
        "borderType": "FULL_BORDER",
        "borderColor": "#90CAF9",
        "clean": False,
    },
}

_STYLE_CHUNK_ROWS = 500
_STYLE_BATCH_SIZE = 40


# ============================================================
# 4. FeishuSheetExporter  —  核心导出引擎（与业务无关）
# ============================================================
class FeishuSheetExporter:
    """通用飞书表格导出器。

    用法：
        exporter = FeishuSheetExporter(spreadsheet_token, config)
        exporter.export(sheet_name, rows)          # rows: List[Dict]
        exporter.append(sheet_name, rows)          # 追加到已有 sheet

    Parameters:
        token:   飞书 Spreadsheet 的 token。
        config:  SheetExportConfig 实例。
    """

    def __init__(
        self, token: str, config: SheetExportConfig, obj_token: str | None = None
    ):
        self.token = token
        self.obj_token = obj_token or token
        self.config = config
        self.client = get_client()
        self.fs_spreadsheet = FeishuSpreadSheet(self.client, token)

    # ------------------------------------------------------------------
    # Public API — 全量导出
    # ------------------------------------------------------------------
    def export(self, sheet_name: str, rows: list[dict[str, Any]]) -> None:
        """导出一个 sheet。

        Args:
            sheet_name: 新建 sheet 的名称。
            rows:       查询结果，每条为 dict（列名 → 值）。
        """
        sheet = self.create_sheet(sheet_name)
        self.db_to_feishu(sheet.raw_sheet.sheet_id, rows, [], only_api=True)

    def create_sheet(self, sheet_name: str) -> FeishuSheet:
        """导出一个 sheet。
        Args:
            sheet_name: 新建 sheet 的名称。
            rows:       查询结果，每条为 dict（列名 → 值）。
        """
        sheet: FeishuSheet | None = None
        try:
            sheet = self._create_sheet(sheet_name)
            if not sheet:
                raise RuntimeError(f"创建 sheet '{sheet_name}' 失败")

            sleep(3)

            # 1. 构建二维数组并写入
            table = self._build_table([])
            self._write_data(sheet, table)

            self._refresh_sheet_grid(sheet)

            indexes = self._get_header_indices_v2(sheet)

            self._set_sheet_freeze(sheet)
            # 5. 列宽
            self._set_column_widths(sheet)

            logger.info("设置保护列...")
            self._set_protected_cols(sheet, indexes)

            self._set_styles(sheet, 1, [])

            logger.info(f"✓ sheet '{sheet_name}' 创建完成")
            return sheet
        except RuntimeError:
            self._rollback_sheet(sheet)
            raise

    # ------------------------------------------------------------------
    # Public API — 同步更新（按主键 + 更新时间）
    # ------------------------------------------------------------------
    def db_to_feishu(
        self,
        sheet_id: str,
        db_rows: list[dict[str, Any]],
        update_headers: list[str],
        force_full_update: bool = False,
        only_api: bool = False,
    ) -> int:
        """同步数据库更新到飞书表格（仅更新已有行）。

        Args:
            sheet_id:         目标 sheet 的 ID。
            rows:             数据库最新数据（List[Dict]）。
            primary_key:      主键字段名（对应 row dict key）。
            update_time_key:  更新时间字段名（对应 row dict key），None 表示不做时间判断。
            update_headers:   仅更新指定表头列（None 表示整行覆盖）。
            primary_key_transform: 主键标准化函数，用于匹配表格中主键值。
            force_full_update: True 时跳过更新时间判断，按主键匹配后强制更新所有命中行。

        Returns:
            实际更新的行数。
        """
        if not db_rows:
            logger.warning(f"给 sheet '{sheet_id}' 提供的同步更新数据为空，无需处理")
            return 0

        effective_update_headers = update_headers or []

        # force_full_update 模式下不更新 update_time；
        # 若调用方未指定 update_headers，则默认更新除 update_time 外的所有列。
        if force_full_update and not effective_update_headers:
            effective_update_headers = [
                header
                for header in self.config.headers
                if header != self.config.update_time_header
            ]

        sheet = self._get_sheet_by_id(sheet_id)
        if not sheet:
            raise RuntimeError(f"sheet_id '{sheet_id}' 不存在")

        updated_count = 0

        with logger.contextualize(prefix=f"{sheet.raw_sheet.title}"):
            should_match_existing = (
                bool(db_rows)
                and bool(self.config.primary_header)
                and bool(self.config.update_time_header)
            )

            updates: list[tuple[int, list[str]]] = []
            row_updates: dict[int, dict[str, Any]] = {}
            unmatched_row_indices = set(range(len(db_rows)))

            if should_match_existing:
                header_indices = self._get_special_header_indices(
                    sheet, effective_update_headers
                )
                if not self.config.primary_header or not self.config.update_time_header:
                    raise ValueError("主键或更新时间表头配置缺失")

                pk_cfg = self.config.fields.get(self.config.primary_header)
                if pk_cfg is None:
                    raise ValueError(
                        f"'{self.config.primary_header}' 未找到对应 FieldConfig"
                    )

                pk_col_idx = header_indices.get(self.config.primary_header)
                update_col_idx = header_indices.get(self.config.update_time_header)
                if pk_col_idx is None or update_col_idx is None:
                    raise RuntimeError("不可能发生的错误")

                feishu_rows: tuple[dict[str, int], dict[int, datetime | None]] | None
                if only_api:
                    feishu_rows = self._build_index_from_api(
                        sheet, pk_col_idx, update_col_idx, pk_cfg.transform_to_db
                    )
                else:
                    feishu_rows = self._build_sheet_index_from_csv(
                        sheet_id,
                        self.config.primary_header,
                        self.config.update_time_header,
                        pk_cfg.transform_to_db,
                    )
                    if feishu_rows is None:
                        logger.warning("导出 CSV 失败，改用接口读取")
                        feishu_rows = self._build_index_from_api(
                            sheet, pk_col_idx, update_col_idx, pk_cfg.transform_to_db
                        )

                if feishu_rows is None:
                    raise RuntimeError("读取表格失败，无法进行同步")

                pk_to_row, sheet_update_time = feishu_rows
                if force_full_update:
                    # 全量模式不做更新时间比较，仅保留主键到行号映射。
                    sheet_update_time = {}

                if not pk_to_row:
                    logger.warning("未读取到飞书主键列数据，将仅执行增量追加")

                update_time_key_checked = self.config.fields[
                    self.config.update_time_header
                ].key
                use_update_time_check = (not force_full_update) and bool(
                    update_time_key_checked
                )
                if force_full_update:
                    update_headers_to_write = effective_update_headers
                elif effective_update_headers:
                    update_headers_to_write = list(
                        dict.fromkeys(
                            [*effective_update_headers, self.config.update_time_header]
                        )
                    )
                else:
                    update_headers_to_write = []

                for idx, row in enumerate(db_rows):
                    pk_display = self._resolve_cell(pk_cfg, row, idx)
                    if not pk_display:
                        continue

                    sheet_row = pk_to_row.get(pk_display)

                    if sheet_row is None:
                        continue

                    unmatched_row_indices.discard(idx)

                    should_update = True
                    if use_update_time_check:
                        assert update_time_key_checked is not None
                        db_update_time = self._parse_datetime_value(
                            row.get(update_time_key_checked)
                        )
                        sheet_time = sheet_update_time.get(sheet_row)

                        db_update_time_sec = self._format_update_time(db_update_time)
                        sheet_time_sec = self._format_update_time(sheet_time)

                        if db_update_time_sec is None:
                            should_update = False
                        else:
                            should_update = (
                                sheet_time_sec is None
                                or db_update_time_sec > sheet_time_sec
                            )

                    if not should_update:
                        continue

                    if update_headers_to_write:
                        row_payload = {
                            header: self._resolve_cell(
                                self.config.fields[header], row, idx
                            )
                            for header in update_headers_to_write
                        }
                        # 只要该行发生更新，强制同步飞书 update_time 列，
                        # 避免仅更新业务列时更新时间停留在旧值。
                        if (
                            not force_full_update
                            and self.config.update_time_header not in row_payload
                        ):
                            row_payload[self.config.update_time_header] = (
                                self._resolve_cell(
                                    self.config.fields[self.config.update_time_header],
                                    row,
                                    idx,
                                )
                            )
                        row_updates[sheet_row] = row_payload
                    else:
                        updates.append((sheet_row, self._build_row(row, idx)))

                if update_headers_to_write:
                    if row_updates:
                        logger.info(f"准备更新 {len(row_updates)} 行")
                        write_columns_from_updates(
                            sheet,
                            {
                                header: header_indices[header]
                                for header in update_headers_to_write
                            },
                            row_updates,
                        )
                        updated_count = len(row_updates)
                else:
                    if updates:
                        updates.sort(key=lambda item: item[0])
                        for start_row, data_rows in self._group_row_updates(updates):
                            self._write_data_range(sheet, data_rows, start_row)
                        updated_count = len(updates)

            remain_db_rows = [db_rows[i] for i in sorted(unmatched_row_indices)]

            appended_count = self._append_rows_to_sheet(sheet, remain_db_rows)
            total_changed = updated_count + appended_count
            if total_changed == 0:
                logger.info("没有需要更新或新增的行")
                return 0

            logger.info(
                f"同步更新完成，更新 {updated_count} 行，新增 {appended_count} 行，共 {total_changed} 行"
            )
            return total_changed

    def feishu_to_db(
        self,
        sheet_id: str,
        db_rows: list[dict[str, Any]],
        sync_headers: list[str],
        protect_non_empty_headers: list[str] | None = None,
        db_update_handler: Callable[
            [list[dict[str, Any]]], int | None
        ] = lambda updates: None,
        only_api: bool = False,
    ) -> int:
        """按主键将飞书字段回写到数据库，并同步双方更新时间。

        Args:
            sheet_id:              目标 sheet 的 ID。
            db_rows:               数据库当前数据（List[Dict]）。
            sync_headers:          需要从飞书读取并比对的表头列表。
            protect_non_empty_headers: 受保护表头列表；若 DB 对应字段已有非空值，则忽略飞书回写。
            diff_mode:            差异模式。True 时忽略更新时间比较，仅当业务字段有差异时回写。

        Returns:
            实际更新的行数。

        Notes:
            对于进入同步判定的行（主键命中且通过更新时间判定），
            无论业务字段是否有差异，都会将飞书与 DB 的更新时间刷新为当前时间。
            当 diff_mode=True 时，跳过更新时间判定，仅在业务字段存在差异时才刷新更新时间。
        """

        def _normalize_compare_value(value: Any) -> str:
            if value is None:
                return ""
            if isinstance(value, datetime):
                return value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(value, date):
                return datetime.combine(value, datetime.min.time()).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            parsed = self._parse_datetime_value(value)
            if parsed is not None:
                return parsed.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
            return str(value).strip()

        if not db_rows:
            logger.warning("DB 数据为空，无需从飞书回写")
            return 0

        sheet = self.fs_spreadsheet.find_sheet_by_id(sheet_id)

        header_indices = self._get_special_header_indices(sheet, sync_headers)

        if not self.config.primary_header or not self.config.update_time_header:
            raise ValueError("主键或更新时间表头配置缺失")

        pk_cfg, primary_key = self._get_real_cfg(self.config.primary_header)
        _, update_time_key = self._get_real_cfg(self.config.update_time_header)

        protected_headers = set(protect_non_empty_headers or [])
        if protected_headers:
            invalid_headers = sorted(
                [header for header in protected_headers if header not in sync_headers]
            )
            if invalid_headers:
                raise ValueError(
                    "protect_non_empty_headers 必须是 sync_headers 的子集: "
                    + ", ".join(invalid_headers)
                )

        feishu_headers = [
            self.config.primary_header,
            self.config.update_time_header,
            *sync_headers,
        ]

        if only_api:
            feishu_rows = self._read_sheet_rows_from_api(sheet, feishu_headers)
        else:
            feishu_rows = self._read_sheet_rows_from_csv(sheet_id, feishu_headers)
            if feishu_rows is None:
                logger.warning("导出 CSV 失败，改用接口读取飞书数据")
                feishu_rows = self._read_sheet_rows_from_api(sheet, feishu_headers)

        if not feishu_rows:
            logger.warning("飞书中没有可回写的数据")
            return 0

        db_map: dict[str, dict[str, Any]] = {}
        for idx, db_row in enumerate(db_rows):
            pk_display = self._resolve_cell(pk_cfg, db_row, idx)
            if not pk_display:
                continue

            db_map[pk_display] = db_row

        if not db_map:
            logger.warning("DB 数据中未解析到有效主键，跳过回写")
            return 0

        sync_time = datetime.now().replace(microsecond=0)
        sync_time_text = sync_time.strftime("%Y-%m-%d %H:%M:%S")
        batch_size = 1000

        db_updates: list[dict[str, Any]] = []
        fs_time_updates: dict[int, dict[str, Any]] = {}

        logger.info("开始从飞书回写 DB，模式={}，命中的行会同步更新时间")

        for sheet_row_index, row_data in feishu_rows:
            pk_value = self._resolve_fs_cell(pk_cfg, row_data.copy())
            if pk_value is None:
                logger.warning(
                    f"sheet 行 {sheet_row_index} 主键列 '{self.config.primary_header}' 经过 transform 解析后为空，跳过"
                )
                continue

            db_row = db_map.get(pk_value)
            if db_row is None:
                logger.info(
                    f"sheet 行 {sheet_row_index} 主键 '{pk_value}' 在 DB 中未找到，跳过"
                )
                continue

            changed_payload: dict[str, Any] = {primary_key: db_row.get(primary_key)}
            changed_fields = 0

            for header in sync_headers:
                cfg = self.config.fields[header]
                assert cfg.key is not None

                sheet_value = row_data.get(header)
                sheet_value_normalized = _normalize_compare_value(sheet_value)
                db_vaule_display = self._resolve_cell(cfg, db_row, 0)

                if header in protected_headers and db_vaule_display != "":
                    continue

                if sheet_value_normalized == db_vaule_display:
                    continue

                changed_payload[cfg.key] = self._resolve_fs_cell(
                    cfg, {cfg.header: sheet_value}
                )

                changed_fields += 1

            if changed_fields == 0:
                continue

            changed_payload[update_time_key] = sync_time
            db_updates.append(changed_payload)
            fs_time_updates[sheet_row_index] = {
                self.config.update_time_header: sync_time_text
            }

        if not db_updates:
            logger.info("飞书与 DB 无需回写或同步更新时间")
            return 0

        updated_count = 0
        # 更新到数据库
        for i in range(0, len(db_updates), batch_size):
            chunk_updates = db_updates[i : i + batch_size]
            chunk_result = db_update_handler(chunk_updates)
            if chunk_result is None:
                updated_count += len(chunk_updates)
            else:
                updated_count += chunk_result

        # 更新飞书 update_time 列
        header_row = self._read_header_row(sheet)
        header_indices = self._build_header_index(header_row)
        update_col_idx = header_indices.get(self.config.update_time_header)
        if update_col_idx is None:
            raise RuntimeError(
                f"更新时间表头 '{self.config.update_time_header}' 未在表格中找到"
            )

        update_col_map = {self.config.update_time_header: update_col_idx}
        sorted_time_updates = sorted(fs_time_updates.items(), key=lambda item: item[0])

        for i in range(0, len(sorted_time_updates), batch_size):
            chunk_time_updates = dict(sorted_time_updates[i : i + batch_size])
            write_columns_from_updates(sheet, update_col_map, chunk_time_updates)

        logger.info(f"飞书回写 DB 完成，更新 {updated_count} 行")

        return updated_count

    # ------------------------------------------------------------------
    # Internal — sheet 生命周期
    # ------------------------------------------------------------------
    def _append_rows_to_sheet(
        self,
        sheet: FeishuSheet,
        append_db_rows: list[dict[str, Any]],
    ) -> int:
        """将 rows 追加写入 sheet，从 start_row 行开始（含）。"""
        if not append_db_rows:
            return 0

        rows = []

        for idx, row in enumerate(append_db_rows):
            rows.append(self._build_row(row, idx))

        header_indices = self._get_header_indices_v2(sheet)

        self._refresh_sheet_grid(sheet)

        existing_row_count = self._get_last_non_empty_row(sheet)
        append_start_row = max(existing_row_count + 1, 2)
        last_old_row = (
            self._read_row(sheet, existing_row_count) if existing_row_count >= 2 else []
        )

        self._write_data_rows_by_headers(
            sheet,
            rows,
            append_start_row,
            header_indices,
        )

        append_end_row = append_start_row + len(append_db_rows) - 1

        self._set_data_validation_range(
            sheet,
            append_start_row,
            append_end_row,
            header_indices=header_indices,
        )

        group_ranges = self._merge_appended_cells(
            sheet,
            rows,
            last_old_row,
            existing_row_count,
            append_start_row,
            header_indices=header_indices,
        )
        global_group_offset = self._count_existing_groups(
            sheet, existing_row_count, header_indices=header_indices
        )

        self._set_styles_for_range(
            sheet,
            append_start_row,
            append_end_row,
            group_ranges,
            global_group_offset,
        )

        return len(append_db_rows)

    def _get_last_non_empty_row(self, sheet: FeishuSheet) -> int:
        """返回最后一个有数据的行号（1-indexed；无数据行时返回 0）。"""
        max_rows = self._get_sheet_row_count(sheet)
        if max_rows is None:
            raise RuntimeError("无法获取表格总行数")
        if max_rows < 2:
            return 0

        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            raise RuntimeError("无法获取表格总列数")
        if max_cols < 1:
            raise RuntimeError("表格列数异常: 0")

        end_col = number_to_letters(max_cols)
        chunk_size = 100
        end_row = max_rows

        # 从底部开始，每次读取 100 行，命中后立即返回。
        while end_row >= 2:
            start_row = max(2, end_row - chunk_size + 1)
            cell_range = f"A{start_row}:{end_col}{end_row}"

            try:
                table_values = sheet.get_range_v2(cell_range) or []
            except Exception as e:
                raise RuntimeError(f"读取最后数据行失败: {e}") from e

            for offset in range(len(table_values) - 1, -1, -1):
                row_values = table_values[offset] if table_values[offset] else []
                if any(
                    cell is not None and str(cell).strip() != "" for cell in row_values
                ):
                    return start_row + offset

            end_row = start_row - 1

        return 0

    def _get_real_cfg(self, header: str) -> tuple[FieldConfig, str]:
        cfg = self.config.fields.get(header)
        if cfg is None:
            raise ValueError(f"表头 '{header}' 没有对应的 FieldConfig")
        if cfg.is_real_field() is False:
            raise ValueError(f"表头 '{header}' 的 FieldConfig 没有配置 key，无法解析值")
        key = cfg.key
        if not key:
            raise ValueError(f"表头 '{header}' 的 FieldConfig key 不能为空")
        return cfg, key

    def _set_protected_cols(
        self, sheet: FeishuSheet, header_indices: dict[str, int]
    ) -> None:
        protected = []
        for header, cfg in self.config.fields.items():
            if cfg.protected:
                col_index = header_indices.get(header)
                if col_index is None:
                    continue
                col_index = col_index + 1
                if col_index is not None:
                    protected.append(
                        AddProtectedDimensionRequestModel(
                            dimension=ProtectedDimensionModel(
                                majorDimension="COLUMNS",
                                startIndex=col_index,
                                endIndex=col_index,
                            )
                        )
                    )
        if protected:
            resp = sheet.protected_dimension(protected)
            logger.debug(resp)

    def _get_special_header_indices(
        self, sheet: FeishuSheet, update_headers: list[str]
    ) -> dict[str, int]:
        if not self.config.primary_header or not self.config.update_time_header:
            raise ValueError("主键或更新时间表头配置缺失")

        update_headers = update_headers or []

        self._refresh_sheet_grid(sheet)

        # 构建表格中主键列索引
        header_row = self._read_header_row(sheet)
        header_indices = self._build_header_index(header_row)

        # 检查所需列是否存在
        for header in update_headers + [
            self.config.primary_header,
            self.config.update_time_header,
        ]:
            if header not in self.config.fields.keys():
                logger.info(
                    f"config.fields 中定义的表头: {list(self.config.fields.keys())}"
                )
                raise RuntimeError(f"表头 '{header}' 未在 config.fields 中定义")
            col_idx = header_indices.get(header)
            if col_idx is None:
                raise RuntimeError(f"表头 '{header}' 未在表格中找到")

        logger.info(f"同步列: {update_headers}")

        return header_indices

    def _get_header_indices_v2(self, sheet: FeishuSheet) -> dict[str, int]:
        """读取表头行并构建表头到列索引的映射。 0-based"""

        header_row = self._read_header_row(sheet)
        header_indices = self._build_header_index(header_row)
        return header_indices

    def _format_update_time(self, update_time: datetime) -> datetime | None:
        return update_time.replace(microsecond=0) if update_time is not None else None

    def _refresh_sheet_grid(self, sheet: FeishuSheet) -> int | None:
        retries = 3
        wait_sec = 1
        last_err: Exception | None = None

        for attempt in range(retries):
            try:
                sheet.refresh_raw_sheet()
                col_count = self._get_sheet_col_count(sheet)
                if col_count and col_count > 0:
                    return col_count
            except Exception as e:
                last_err = e

            if attempt < retries - 1:
                logger.info(f"等待 {wait_sec}s 以刷新网格信息")
                sleep(wait_sec)

        if last_err:
            logger.warning(f"刷新 sheet 网格信息失败: {last_err}")

        return self._get_sheet_col_count(sheet)

    def _create_sheet(self, sheet_name: str) -> FeishuSheet | None:
        mapping = self.fs_spreadsheet.add_sheet(sheet_name)
        if sheet_name not in mapping:
            logger.error(f"飞书返回的 sheet 映射中找不到 '{sheet_name}'")
            return None
        sheet_id = mapping[sheet_name]["sheetId"]
        return FeishuSheet(self.client, self.token, sheet_id)

    def _get_existing_sheet(self, sheet_name: str) -> FeishuSheet | None:
        """从 spreadsheet 中查找已有 sheet，返回 FeishuSheet 或 None。"""
        sheet_map = {
            sheet.raw_sheet.title: sheet for sheet in self.fs_spreadsheet.sheets
        }  # {name: {sheetId, ...}}
        if sheet_name not in sheet_map:
            logger.warning(f"sheet '{sheet_name}' 不存在")
            return None
        sheet = sheet_map.get(sheet_name)
        return sheet

    def _set_sheet_freeze(self, sheet: FeishuSheet) -> None:
        if (
            self.config.frozen_row_count is None
            and self.config.frozen_col_count is None
        ):
            return
        updates: list[UpdateSheetRequestModel] = []
        updates.append(
            UpdateSheetRequestModel(
                sheetId=sheet.sheet_id,
                frozenRowCount=self.config.frozen_row_count,
                frozenColCount=self.config.frozen_col_count,
            )
        )
        try:
            self.fs_spreadsheet.update_sheets(updates)
        except Exception as e:
            logger.error(f"设置冻结行列失败: {e}")
            raise

    def _rollback_sheet(self, sheet: FeishuSheet | None):
        if sheet is None:
            return
        try:
            logger.warning("导出异常，尝试删除 sheet 进行回滚")
            self.fs_spreadsheet.del_sheets([sheet.sheet_id])
        except Exception as e:
            logger.error(f"回滚删除 sheet 失败: {e}")

    def _get_sheet_by_id(self, sheet_id: str) -> FeishuSheet | None:
        for sheet in self.fs_spreadsheet.sheets:
            if sheet.sheet_id == sheet_id:
                return sheet
        try:
            return FeishuSheet(self.client, self.token, sheet_id)
        except Exception as e:
            logger.error(f"通过 sheet_id 获取 sheet 失败: {e}")
            return None

    # ------------------------------------------------------------------
    # Internal — 数据构建
    # ------------------------------------------------------------------
    def _build_table(self, rows: list[dict[str, Any]]) -> list[list[str]]:
        """将 rows 转为 [header_row, data_row, ...]。

        每个 FieldConfig 的值解析顺序：
            1. 有 transform  →  调用 transform(row, index)
            2. 有 key        →  row[key]，再格式化
            3. 都没有        →  空字符串
        """
        headers = self.config.headers
        table: list[list[str]] = [headers]

        for idx, row in enumerate(rows):
            cells: list[str] = []
            for header in headers:
                cfg = self.config.fields[header]
                cells.append(self._resolve_cell(cfg, row, idx))
            table.append(cells)

        return table

    def _build_row(self, row: dict[str, Any], index: int) -> list[str]:
        """
        将单行数据转为 [cell1, cell2, ...]，顺序对应 config.headers。
        index 只是用来提供行号信息给 transform 函数
        """

        headers = self.config.headers
        cells: list[str] = []
        for header in headers:
            cfg = self.config.fields[header]
            cells.append(self._resolve_cell(cfg, row, index))
        return cells

    def _resolve_cell(self, cfg: FieldConfig, row: dict[str, Any], index: int) -> str:
        """解析单个单元格的值，并应用 FieldConfig 的字段修饰。"""
        # 1) 优先使用自定义转换函数
        if cfg.transform is not None:
            raw_value = cfg.transform(row, index)
        else:
            # 2) 无 key → 空
            if cfg.key is None:
                # logger.warning(
                #     f"FieldConfig for header '{cfg.header}' 没有配置 key，且 transform 也不存在，将使用空字符串"
                # )
                return ""
            raw_value = row.get(cfg.key)

        # 3) 按 field_type 做值格式化
        value = self._format_cell_value(cfg, raw_value)
        return value
        # 4) 按 validation 做值规范化（如大小写/空白等）
        # return self._normalize_cell_by_validation(cfg, value)

    def _resolve_fs_cell(self, cfg: FieldConfig, row: dict[str, Any]) -> str | None:
        if not cfg.key:
            raise ValueError(
                "FieldConfig.key is required to resolve cell value from row"
            )
        value = row.get(cfg.header)

        if cfg.transform_to_db:
            return cfg.transform_to_db(value)
        return value

    def _format_cell_value(self, cfg: FieldConfig, value: Any) -> Any:
        if value is None:
            return ""

        if cfg.field_type == "date":
            parsed = self._parse_datetime_value(value)
            if parsed is not None:
                if parsed.time() == datetime.min.time():
                    return parsed.strftime("%Y-%m-%d")
                return parsed.strftime("%Y-%m-%d %H:%M:%S")

        # 非 date 字段保留原有行为：datetime/date 自动转可读字符串。
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")

        return value

    def _normalize_cell_by_validation(self, cfg: FieldConfig, value: str) -> str:
        if not value:
            return value

        validation = cfg.validation
        if not isinstance(validation, dict):
            return value

        condition_values = validation.get("condition_values")
        if not isinstance(condition_values, list) or not condition_values:
            return value

        canonical_map: dict[str, str] = {}
        for item in condition_values:
            text = str(item).strip()
            if not text:
                continue
            key = text.lower()
            if key not in canonical_map:
                canonical_map[key] = text

        if not canonical_map:
            return value

        options = validation.get("options")
        allow_multiple = isinstance(options, dict) and bool(
            options.get("multipleValues")
        )

        if allow_multiple:
            raw_parts = str(value).replace("，", ",").split(",")
            parts: list[str] = []
            for part in raw_parts:
                item = part.strip()
                if not item:
                    continue
                parts.append(canonical_map.get(item.lower(), item))
            return ",".join(parts)

        trimmed = str(value).strip()
        return canonical_map.get(trimmed.lower(), trimmed)

    # ------------------------------------------------------------------
    # Internal — 写入
    # ------------------------------------------------------------------
    def _write_data(self, sheet: FeishuSheet, table: list[list[str]]):
        """写入完整表格（含 header），用于 export。"""
        num_rows = len(table)
        num_cols = len(self.config.headers)
        end_col = number_to_letters(num_cols)
        cell_range = f"A1:{end_col}{num_rows}"
        try:
            logger.info(sheet.write_range(cell_range, table))
            logger.info(f"写入完成：{num_rows - 1} 行数据")
        except Exception as e:
            logger.error(f"写入数据失败: {e}")
            raise

    def _write_data_range(
        self,
        sheet: FeishuSheet,
        data_rows: list[list[str]],
        start_row: int,
        batch_size: int = 1000,
    ):
        """从指定行开始写入数据（不含 header），用于 append。

        为避免单次请求过大，默认按 1000 行分批写入。
        """
        if not data_rows:
            return
        if batch_size < 1:
            raise ValueError("batch_size 必须大于 0")

        num_cols = len(self.config.headers)
        end_col = number_to_letters(num_cols)

        for i in range(0, len(data_rows), batch_size):
            chunk_rows = data_rows[i : i + batch_size]
            chunk_start = start_row + i
            chunk_end = chunk_start + len(chunk_rows) - 1
            cell_range = f"A{chunk_start}:{end_col}{chunk_end}"
            try:
                sheet.write_range(cell_range, chunk_rows)
                logger.info(f"追加写入完成：行 {chunk_start}~{chunk_end}")
            except Exception as e:
                logger.error(f"追加写入失败: {e}")
                raise

    def _write_data_rows_by_headers(
        self,
        sheet: FeishuSheet,
        data_rows: list[list[str]],
        start_row: int,
        header_indices: dict[str, int] | None = None,
        batch_size: int = 1000,
    ) -> None:
        """按当前 sheet 表头位置写入数据（用于列被手工调换后的追加）。"""
        if not data_rows:
            return
        if batch_size < 1:
            raise ValueError("batch_size 必须大于 0")

        if not header_indices:
            self._write_data_range(
                sheet,
                data_rows,
                start_row,
                batch_size=batch_size,
            )
            return

        written_cols = 0
        for i in range(0, len(data_rows), batch_size):
            chunk_rows = data_rows[i : i + batch_size]
            chunk_start = start_row + i
            chunk_end = chunk_start + len(chunk_rows) - 1

            for data_col_idx, header in enumerate(self.config.headers):
                sheet_col_idx = header_indices.get(header)
                if sheet_col_idx is None:
                    logger.warning(f"表头 '{header}' 未在 sheet 中找到，跳过该列写入")
                    continue

                col_letter = number_to_letters(sheet_col_idx + 1)
                cell_range = f"{col_letter}{chunk_start}:{col_letter}{chunk_end}"
                col_values = [[row[data_col_idx]] for row in chunk_rows]

                try:
                    sheet.write_range(cell_range, col_values)
                except Exception as e:
                    logger.error(f"按表头追加写入失败: {header} [{cell_range}] | {e}")
                    raise

                written_cols += 1

        if written_cols == 0:
            raise RuntimeError("未匹配到任何可写入表头，追加失败")

        end_row = start_row + len(data_rows) - 1
        logger.info(f"追加写入完成：行 {start_row}~{end_row}，写入列数 {written_cols}")

    # ------------------------------------------------------------------
    # Internal — 数据验证
    # ------------------------------------------------------------------
    def _set_data_validation(
        self, sheet: FeishuSheet, num_rows: int, header_indices: dict[str, int]
    ):
        """对整个数据区域设置验证，用于 export。"""
        if num_rows <= 1:
            return
        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            logger.warning("无法获取网格列数，跳过数据验证")
            return
        for name, cfg in self.config.fields.items():
            if cfg.validation is None:
                continue
            col_index = header_indices.get(name)
            if col_index is None:
                logger.warning(f"表头 '{name}' 未在 sheet 中找到，跳过数据验证")
                continue
            col_index += 1
            if max_cols is not None and col_index > max_cols:
                logger.warning(
                    f"数据验证列超出网格列数，跳过: {name} (col={col_index})"
                )
                continue
            col_letter = number_to_letters(col_index)
            cell_range = f"{col_letter}2:{col_letter}{num_rows}"
            try:
                self._retry_on_range_error(
                    op=lambda: sheet.set_data_validation(
                        cell_range=cell_range,
                        condition_values=cfg.validation["condition_values"],
                        options=cfg.validation.get("options", {}),
                    ),
                    desc=f"设置数据验证 {name} [{cell_range}]",
                    sheet=sheet,
                )
                logger.info(f"数据验证设置完成：{name}")
            except Exception as e:
                logger.error(f"设置 '{name}' 数据验证失败: {e}")
                raise

    def _set_data_validation_range(
        self,
        sheet: FeishuSheet,
        start_row: int,
        end_row: int,
        header_indices: dict[str, int],
    ):
        """仅对新增行区间设置数据验证，用于 append。"""
        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            logger.warning("无法获取网格列数，跳过追加数据验证")
            return
        for name, cfg in self.config.fields.items():
            if cfg.validation is None:
                continue

            col_idx0 = header_indices.get(name)
            if col_idx0 is None:
                logger.warning(f"追加数据验证列未在表格中找到，跳过: {name}")
                continue
            col_index = col_idx0 + 1

            if max_cols is not None and col_index > max_cols:
                logger.warning(
                    f"追加数据验证列超出网格列数，跳过: {name} (col={col_index})"
                )
                continue
            col_letter = number_to_letters(col_index)
            cell_range = f"{col_letter}{start_row}:{col_letter}{end_row}"
            try:
                self._retry_on_range_error(
                    op=lambda: sheet.set_data_validation(
                        cell_range=cell_range,
                        condition_values=cfg.validation["condition_values"],
                        options=cfg.validation.get("options", {}),
                    ),
                    desc=f"追加数据验证 {name} [{cell_range}]",
                    sheet=sheet,
                )
                logger.info(f"追加数据验证设置完成：{name} [{cell_range}]")
            except Exception as e:
                logger.error(f"追加设置 '{name}' 数据验证失败: {e}")
                raise

    # ------------------------------------------------------------------
    # Internal — 合并单元格
    # ------------------------------------------------------------------
    def _merge_grouped_cells(
        self,
        sheet: FeishuSheet,
        table: list[list[str]],
    ) -> list[tuple[int, int]]:
        """按 group_by_key 分组后合并 merge_columns 范围内的单元格。用于 export。

        Returns:
            每个分组的 (start_row_1indexed, end_row_1indexed) 列表，
            供后续条纹样式使用。
        """
        if self.config.group_by_key is None or len(table) <= 1:
            return []

        headers = self.config.headers
        merge_cols = self.config.resolved_merge_columns
        if not merge_cols:
            return []

        # 找到 group_by 列在 table 中的索引
        # group_by_key 是 row dict 的 key，需要找到哪个 FieldConfig.key == group_by_key
        group_col_idx = self._find_col_index_by_key(self.config.group_by_key)
        if group_col_idx is None:
            logger.warning(
                f"group_by_key '{self.config.group_by_key}' 未在任何 FieldConfig.key 中找到，跳过合并"
            )
            return []

        # 合并列的起止索引（0-based）
        merge_start = headers.index(merge_cols[0])
        merge_end = headers.index(merge_cols[-1])

        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            logger.warning("无法获取网格列数，跳过合并")
            return []
        if max_cols is not None:
            if merge_start + 1 > max_cols:
                logger.warning("合并列起始超出网格列数，跳过合并")
                return []
            if merge_end + 1 > max_cols:
                merge_end = max_cols - 1

        # 扫描分组边界
        group_ranges: list[tuple[int, int]] = (
            []
        )  # (start, end) 均为 1-indexed sheet 行号
        current_val = None
        start_row = None  # 1-indexed

        for row_idx in range(1, len(table)):  # 跳过 header
            val = table[row_idx][group_col_idx]
            if val != current_val:
                # 前一组结束 → 合并 + 记录
                if current_val is not None and start_row is not None:
                    end_row = row_idx  # 不含当前行
                    group_ranges.append((start_row, end_row))
                    if end_row > start_row:
                        self._do_merge(
                            sheet, merge_start, merge_end, start_row, end_row
                        )

                current_val = val
                start_row = row_idx + 1  # 转为 1-indexed

        # 最后一组
        if current_val is not None and start_row is not None:
            end_row = len(table)
            group_ranges.append((start_row, end_row))
            if end_row > start_row:
                self._do_merge(sheet, merge_start, merge_end, start_row, end_row)

        return group_ranges

    def _merge_appended_cells(
        self,
        sheet: FeishuSheet,
        new_data_rows: list[list[str]],
        last_old_row: list[str],
        existing_row_count: int,
        append_start_row: int,
        header_indices: dict[str, int],
    ) -> list[tuple[int, int]]:
        """处理新数据的分组合并，并衔接旧末尾组。用于 append。

        Returns:
            新数据产生的分组范围列表 [(start_row, end_row), ...]（1-indexed），
            供样式交替色使用。
        """
        if self.config.group_by_key is None:
            return []

        merge_cols = self.config.resolved_merge_columns
        if not merge_cols:
            return []

        group_col_idx_in_data = self._find_col_index_by_key(self.config.group_by_key)
        if group_col_idx_in_data is None:
            logger.warning(
                f"group_by_key '{self.config.group_by_key}' 未找到，跳过追加合并"
            )
            return []

        group_header = self._find_header_by_key(self.config.group_by_key)
        if group_header is None:
            logger.warning(
                f"group_by_key '{self.config.group_by_key}' 未映射到表头，跳过追加合并"
            )
            return []
        group_col_idx_in_sheet = header_indices.get(group_header)
        if group_col_idx_in_sheet is None:
            logger.warning(f"分组表头 '{group_header}' 未在 sheet 中找到，跳过追加合并")
            return []

        merge_indices_opt = [header_indices.get(name) for name in merge_cols]
        if any(index is None for index in merge_indices_opt):
            logger.warning("部分合并列未在 sheet 中找到，跳过追加合并")
            return []
        merge_indices = [index for index in merge_indices_opt if index is not None]
        merge_start = min(merge_indices)
        merge_end = max(merge_indices)

        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            logger.warning("无法获取网格列数，跳过追加合并")
            return []
        if max_cols is not None:
            if merge_start + 1 > max_cols:
                logger.warning("追加合并列起始超出网格列数，跳过合并")
                return []
            if merge_end + 1 > max_cols:
                merge_end = max_cols - 1

        # 旧数据末尾的分组值
        old_last_val = (
            last_old_row[group_col_idx_in_sheet]
            if group_col_idx_in_sheet < len(last_old_row)
            else None
        )

        group_ranges: list[tuple[int, int]] = []
        current_val = None
        start_row = None  # 1-indexed

        logger.info("合并单元格...")
        for i, row in enumerate(new_data_rows):
            sheet_row = append_start_row + i  # 当前行在 sheet 中的 1-indexed 行号
            val = row[group_col_idx_in_data]

            if val != current_val:
                # 结束前一个新数据组
                if current_val is not None and start_row is not None:
                    end_row = sheet_row - 1
                    group_ranges.append((start_row, end_row))
                    if end_row > start_row:
                        self._do_merge(
                            sheet, merge_start, merge_end, start_row, end_row
                        )

                current_val = val

                # 关键衔接逻辑：新数据首行与旧末尾同组 → 合并范围从旧组开始
                if i == 0 and val == old_last_val and old_last_val is not None:
                    # 找到旧组的起始行（向上扫描旧数据中同值的连续行）
                    start_row = self._find_group_start_row(
                        sheet,
                        group_col_idx_in_sheet,
                        existing_row_count,
                        old_last_val,
                    )
                else:
                    start_row = sheet_row

        # 最后一组
        if current_val is not None and start_row is not None:
            end_row = append_start_row + len(new_data_rows) - 1
            group_ranges.append((start_row, end_row))
            if end_row > start_row:
                self._do_merge(sheet, merge_start, merge_end, start_row, end_row)

        return group_ranges

    def _find_col_index_by_key(self, key: str) -> int | None:
        """根据 FieldConfig.key 找到对应的列索引（0-based in table）。"""
        for idx, (_, cfg) in enumerate(self.config.fields.items()):
            if cfg.key == key:
                return idx
        return None

    def _find_group_start_row(
        self,
        sheet: FeishuSheet,
        group_col_idx: int,
        existing_row_count: int,
        target_val: str,
    ) -> int:
        """从 existing_row_count 行往上扫描，找到连续同值组的起始行（1-indexed）。

        用于追加时衔接旧末尾组：新数据首行与旧末尾值相同时，
        需要将合并范围延伸到旧组起点。
        """
        col_letter = number_to_letters(group_col_idx + 1)
        # 批量读取该列数据行（跳过 header）
        cell_range = f"{col_letter}2:{col_letter}{existing_row_count}"
        col_data = sheet.read_range(cell_range)  # [[val], [val], ...]

        # 从末尾往前找第一个不同值的位置
        start = existing_row_count  # 默认从最后一行开始（1-indexed）
        for i in range(len(col_data) - 1, -1, -1):
            cell_val = col_data[i][0] if col_data[i] else ""
            if cell_val != target_val:
                break
            start = i + 2  # +2：跳过 header 的偏移(+1) + 转1-indexed(+1)

        return start

    def _do_merge(
        self,
        sheet: FeishuSheet,
        start_col: int,
        end_col: int,
        start_row: int,
        end_row: int,
    ):
        start_letter = number_to_letters(start_col + 1)
        end_letter = number_to_letters(end_col + 1)
        cell_range = f"{start_letter}{start_row}:{end_letter}{end_row}"
        try:
            self._retry_on_range_error(
                op=lambda: sheet.merge_cells(cell_range, "MERGE_COLUMNS"),
                desc=f"合并单元格 {cell_range}",
                sheet=sheet,
            )
            # logger.info(f"合并完成: {cell_range}")
        except Exception as e:
            logger.error(f"合并 {cell_range} 失败: {e}")

    # ------------------------------------------------------------------
    # Internal — 读取辅助
    # ------------------------------------------------------------------
    def _read_row(self, sheet: FeishuSheet, row_number: int) -> list[str]:
        """读回指定行号（1-indexed）的全部单元格值。"""
        max_cols = self._get_sheet_col_count(sheet)
        num_cols = (
            max_cols
            if max_cols is not None and max_cols > 0
            else len(self.config.headers)
        )
        end_col = number_to_letters(num_cols)
        cell_range = f"A{row_number}:{end_col}{row_number}"
        data = sheet.get_range_v2(cell_range)
        return data[0] if data else []

    def _read_header_row(self, sheet: FeishuSheet) -> list[Any]:
        """
        读回表头行（默认第一行）的全部单元格值。
        """
        max_cols = self._get_sheet_col_count(sheet)
        desired_cols = (
            max_cols
            if max_cols is not None and max_cols > 0
            else len(self.config.headers)
        )
        end_col = self._resolve_end_col_letter(sheet, desired_cols)
        if end_col is None:
            return []
        cell_range = f"A1:{end_col}1"
        data = sheet.get_range_v2(cell_range)
        return data[0] if data else []

    @staticmethod
    def _build_header_index(header_row: list[Any]) -> dict[str, int]:
        """ "
        根据表头行构建 {header_name: col_index} 映射，col_index 从 0 开始。
        """
        indices: dict[str, int] = {}
        for idx, cell in enumerate(header_row):
            name = str(cell).strip() if cell is not None else ""
            if name:
                indices[name] = idx
        return indices

    def _read_column_values(
        self, sheet: FeishuSheet, col_index: int, start_row: int, end_row: int
    ) -> list[str | None]:
        if start_row > end_row:
            return []
        col_letter = number_to_letters(col_index)
        cell_range = f"{col_letter}{start_row}:{col_letter}{end_row}"
        values = sheet.get_range_v2(cell_range) or []
        result: list[str | None] = []
        for row in values:
            if not row:
                result.append(None)
                continue
            result.append(row[0])
        return result

    def _safe_get_cell(self, row_values: list[Any], col_index: int) -> Any:
        if col_index < 0:
            return None
        if col_index >= len(row_values):
            return None
        value = row_values[col_index]

        if isinstance(value, list):
            if "'link'" in str(value):
                return value[0]["text"]
            raise RuntimeError(f"检查到单元格的值为 list: {value}")

        if isinstance(value, dict):
            raise RuntimeError(f"检查到单元格的值为 dict: {value}")

        return row_values[col_index]

    def _build_sheet_index_from_csv(
        self,
        sheet_id: str,
        pk_header: str,
        update_header: str,
        primary_key_transform: Callable[[Any], str] | None = None,
    ) -> tuple[dict[str, int], dict[int, datetime | None]] | None:
        csv_path = self._download_sheet_csv(sheet_id)
        if csv_path is None:
            return None

        pk_to_row: dict[str, int] = {}
        sheet_update_time: dict[int, datetime | None] = {}

        try:
            with open(csv_path, encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader, None)
                if not header:
                    logger.warning("导出 CSV 为空，跳过同步")
                    return None

                try:
                    pk_idx = header.index(pk_header)
                    update_idx = header.index(update_header)
                except ValueError:
                    logger.warning("导出 CSV 缺少关键表头，改用接口读取")
                    return None

                for i, row in enumerate(reader):
                    row_index = 2 + i
                    pk_val = self._safe_get_cell(row, pk_idx)
                    if pk_val is None or str(pk_val).strip() == "":
                        continue
                    key = str(pk_val).strip()
                    if primary_key_transform is not None:
                        key = primary_key_transform(key)
                    if not key:
                        continue
                    pk_to_row[key] = row_index

                    update_val = self._safe_get_cell(row, update_idx)
                    sheet_update_time[row_index] = self._parse_datetime_value(
                        update_val
                    )
        except Exception as e:
            logger.error(f"读取导出 CSV 失败: {e}")
            return None

        return pk_to_row, sheet_update_time

    def _build_sheet_pk_index_from_export(
        self,
        sheet_id: str,
        pk_header: str,
        primary_key_transform: Callable[[Any], str] | None = None,
    ) -> dict[str, int] | None:
        csv_path = self._download_sheet_csv(sheet_id)
        if csv_path is None:
            return None

        pk_to_row: dict[str, int] = {}

        try:
            with open(csv_path, encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader, None)
                if not header:
                    logger.warning("导出 CSV 为空，跳过同步")
                    return None

                try:
                    pk_idx = header.index(pk_header)
                except ValueError:
                    logger.warning("导出 CSV 缺少关键表头，改用接口读取")
                    return None

                for i, row in enumerate(reader):
                    row_index = 2 + i
                    pk_val = self._safe_get_cell(row, pk_idx)
                    if pk_val is None or str(pk_val).strip() == "":
                        continue
                    key = str(pk_val).strip()
                    if primary_key_transform is not None:
                        key = primary_key_transform(key)
                    if not key:
                        continue
                    pk_to_row[key] = row_index
        except Exception as e:
            logger.error(f"读取导出 CSV 失败: {e}")
            return None

        return pk_to_row

    def _build_index_from_api(
        self,
        sheet: FeishuSheet,
        pk_col_idx: int,
        update_col_idx: int,
        primary_key_transform: Callable[[Any], str] | None = None,
    ) -> tuple[dict[str, int], dict[int, datetime | None]] | None:
        max_rows = self._get_sheet_row_count(sheet)
        if max_rows is None or max_rows < 2:
            return None

        end_col = self._resolve_end_col_letter(sheet, len(self.config.headers))
        if end_col is None:
            return None

        cell_range = f"A1:{end_col}{max_rows}"
        table_values = sheet.get_range_v2(cell_range) or []
        if len(table_values) < 2:
            return None

        data_rows = table_values[1:]

        pk_to_row: dict[str, int] = {}
        sheet_update_time: dict[int, datetime | None] = {}
        for i, row_values in enumerate(data_rows):
            row_index = 2 + i
            pk_val = self._safe_get_cell(row_values, pk_col_idx)

            if pk_val is None or str(pk_val).strip() == "":
                continue
            key = str(pk_val).strip()
            if primary_key_transform is not None:
                key = primary_key_transform(key)
            if not key:
                continue
            pk_to_row[key] = row_index

            update_val = self._safe_get_cell(row_values, update_col_idx)
            sheet_update_time[row_index] = self._parse_datetime_value(update_val)

        return pk_to_row, sheet_update_time

    def _build_sheet_pk_index_from_sheet(
        self,
        sheet: FeishuSheet,
        pk_col_idx: int,
        primary_key_transform: Callable[[Any], str] | None = None,
    ) -> dict[str, int]:
        max_rows = self._get_sheet_row_count(sheet)
        if max_rows is None or max_rows < 2:
            return {}

        end_col = self._resolve_end_col_letter(sheet, len(self.config.headers))
        if end_col is None:
            return {}

        cell_range = f"A1:{end_col}{max_rows}"
        table_values = sheet.get_range_v2(cell_range) or []
        if len(table_values) < 2:
            return {}

        data_rows = table_values[1:]
        pk_to_row: dict[str, int] = {}
        for i, row_values in enumerate(data_rows):
            row_index = 2 + i
            pk_val = self._safe_get_cell(row_values, pk_col_idx)
            if pk_val is None or str(pk_val).strip() == "":
                continue
            key = str(pk_val).strip()
            if primary_key_transform is not None:
                key = primary_key_transform(key)
            if not key:
                continue
            pk_to_row[key] = row_index
        return pk_to_row

    def _read_sheet_rows_from_csv(
        self,
        sheet_id: str,
        headers: list[str],
    ) -> list[tuple[int, dict[str, Any]]] | None:
        csv_path = self._download_sheet_csv(sheet_id)
        if csv_path is None:
            return None

        result: list[tuple[int, dict[str, Any]]] = []

        try:
            with open(csv_path, encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                csv_header = next(reader, None)
                if not csv_header:
                    return []

                header_indices: dict[str, int] = {}
                for header in headers:
                    if header not in csv_header:
                        logger.warning(f"导出 CSV 不包含表头 '{header}'")
                        return None
                    header_indices[header] = csv_header.index(header)

                for i, row in enumerate(reader):
                    row_index = 2 + i
                    row_map: dict[str, Any] = {}
                    has_non_empty = False
                    for header, col_idx in header_indices.items():
                        cell = self._safe_get_cell(row, col_idx)
                        if cell is not None and str(cell).strip() != "":
                            has_non_empty = True
                        row_map[header] = cell
                    if has_non_empty:
                        result.append((row_index, row_map))
        except Exception as e:
            logger.error(f"读取导出 CSV 行数据失败: {e}")
            return None

        return result

    def _read_sheet_rows_from_api(
        self,
        sheet: FeishuSheet,
        headers: list[str],
    ) -> list[tuple[int, dict[str, Any]]]:
        """
        从 sheet 读取行数据，返回包含指定 headers 的行列表。用于同步前读取现有数据。

        return: List of (row_index, {header: cell_value, ...})，row_index 从 1 开始计数。
        """
        max_rows = self._get_sheet_row_count(sheet)
        if max_rows is None or max_rows < 2:
            return []

        header_row = self._read_header_row(sheet)
        header_indices = self._build_header_index(header_row)

        missing = [header for header in headers if header not in header_indices]
        if missing:
            raise RuntimeError(f"表格缺少表头: {', '.join(missing)}")

        end_col = self._resolve_end_col_letter(sheet, len(header_row))
        if end_col is None:
            return []

        table_values = sheet.get_range_v2(f"A1:{end_col}{max_rows}") or []
        if len(table_values) < 2:
            return []

        result: list[tuple[int, dict[str, Any]]] = []
        for i, row_values in enumerate(table_values[1:]):
            row_index = 2 + i
            row_map: dict[str, Any] = {}
            has_non_empty = False
            # 遍历行的指定表头列，构建 {header: cell_value} 映射，并检查是否有非空值
            for header in headers:
                col_idx = header_indices[header]
                value = self._safe_get_cell(row_values, col_idx)
                if value is not None and str(value).strip() != "":
                    has_non_empty = True
                row_map[header] = value
            if has_non_empty:
                result.append((row_index, row_map))

        return result

    def _download_sheet_csv(self, sheet_id: str) -> Path | None:
        exporter = FeishuDocExporter(self.client)
        tmp_dir = tempfile.gettempdir()
        try:
            return exporter.export_and_download(
                self.obj_token,
                FeishuFileExtension.CSV,
                FeishuDocType.SHEET,
                tmp_dir,
                sub_id=sheet_id,
            )
        except Exception as e:
            logger.error(f"导出 CSV 失败: {e}")
            return None

    def _find_header_by_key(self, key: str) -> str | None:
        for header, cfg in self.config.fields.items():
            if cfg.key == key:
                return header
        return None

    def _group_row_updates(
        self, updates: list[tuple[int, list[str]]]
    ) -> list[tuple[int, list[list[str]]]]:
        grouped: list[tuple[int, list[list[str]]]] = []
        start_row: int | None = None
        batch: list[list[str]] = []
        prev_row: int | None = None

        for row_index, row_values in updates:
            if start_row is None:
                start_row = row_index
                batch = [row_values]
                prev_row = row_index
                continue

            if prev_row is not None and row_index == prev_row + 1:
                batch.append(row_values)
            else:
                grouped.append((start_row, batch))
                start_row = row_index
                batch = [row_values]
            prev_row = row_index

        if start_row is not None and batch:
            grouped.append((start_row, batch))

        return grouped

    def _parse_datetime_value(self, value: Any) -> datetime | None:
        raw_value = value
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None

            normalized = (
                value.replace("年", "-")
                .replace("月", "-")
                .replace("日", "")
                .replace("/", "-")
            )

            # 优先尝试 ISO 格式（例如 2026-02-28T10:20:30 / 带时区）
            try:
                iso_value = normalized.replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso_value)
                return dt.replace(tzinfo=None)
            except ValueError:
                pass

            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(normalized, fmt)
                except ValueError:
                    continue

        logger.debug(
            "无法解析时间值 | value={} | type={}",
            raw_value,
            type(raw_value).__name__,
        )
        return None

    def _find_field_config_by_key(self, key: str) -> FieldConfig | None:
        for cfg in self.config.fields.values():
            if cfg.key == key:
                return cfg
        return None

    def _count_existing_groups(
        self,
        sheet: FeishuSheet,
        existing_row_count: int,
        header_indices: dict[str, int],
    ) -> int:
        """统计已有数据中分组数量，用于交替色偏移。

        无 group_by_key 时返回 0。
        """
        if self.config.group_by_key is None or existing_row_count <= 1:
            return 0

        group_col_idx_in_data = self._find_col_index_by_key(self.config.group_by_key)
        if group_col_idx_in_data is None:
            return 0

        group_header = self._find_header_by_key(self.config.group_by_key)
        if group_header is None:
            return 0
        group_col_idx = header_indices.get(group_header)
        if group_col_idx is None:
            return 0

        col_letter = number_to_letters(group_col_idx + 1)
        cell_range = f"{col_letter}2:{col_letter}{existing_row_count}"
        col_data = sheet.get_range(cell_range)

        if not col_data:
            return 0

        # 数不同值的变化次数 + 1 = 分组数
        group_count = 1
        prev = col_data[0][0] if col_data[0] else ""
        for row in col_data[1:]:
            val = row[0] if row else ""
            if val != prev:
                group_count += 1
                prev = val
        return group_count

    # ------------------------------------------------------------------
    # Internal — 样式
    # ------------------------------------------------------------------
    def _set_styles(
        self,
        sheet: FeishuSheet,
        num_rows: int,
        group_ranges: list[tuple[int, int]],
    ):
        """对整个表格设置样式，用于 export。"""
        sheet_id = sheet.sheet_id
        headers = self.config.headers
        end_col = self._resolve_end_col_letter(sheet, len(headers))
        if end_col is None:
            return

        style_batch: list[dict] = []

        # 1. header 行
        self._append_row_style(
            style_batch,
            sheet_id,
            "A",
            end_col,
            1,
            1,
            STYLE_CONFIG["header"],
        )

        if num_rows > 0:
            max_cols = self._get_sheet_col_count(sheet)
            logger.info(
                f"样式列范围: A1:{end_col} (grid_cols={max_cols}, headers={len(headers)})"
            )
            header_indices = self._get_header_indices_v2(sheet)
            # 2. 全体数据行基础样式
            self._append_row_style(
                style_batch,
                sheet_id,
                "A",
                end_col,
                2,
                num_rows,
                STYLE_CONFIG["data"],
            )
            # 3. 按 field_type 分类样式（number / date 可扩展）
            for name, cfg in self.config.fields.items():
                if cfg.field_type in ("number", "date"):
                    col_index = headers.index(name) + 1
                    if not self._col_in_grid(sheet, col_index):
                        continue
                    col = number_to_letters(col_index)
                    # number / date 目前样式与 data 相同，保留扩展点
                    self._append_row_style(
                        style_batch,
                        sheet_id,
                        col,
                        col,
                        2,
                        num_rows,
                        STYLE_CONFIG["data"],
                    )

            # 4. 分组条纹（交替行背景）
            for idx, (s, e) in enumerate(group_ranges):
                style = (
                    STYLE_CONFIG["merged"]
                    if idx % 2 == 0
                    else STYLE_CONFIG["merged_alternate"]
                )
                self._append_row_style(
                    style_batch,
                    sheet_id,
                    "A",
                    end_col,
                    s,
                    e,
                    style,
                )

        # 应用
        try:
            self._apply_style_batches(sheet, style_batch, "批量设置样式")
        except Exception as e:
            logger.error(f"设置样式失败: {e}")
            raise

    def _set_styles_for_range(
        self,
        sheet: FeishuSheet,
        start_row: int,
        end_row: int,
        group_ranges: list[tuple[int, int]],
        global_group_offset: int,
    ):
        """对新增行应用样式，用于 append。交替色基于全局分组序号连续编号。"""
        if start_row > end_row:
            return

        headers = self.config.headers
        sheet_id = sheet.sheet_id
        end_col = self._resolve_end_col_letter(sheet, len(headers))
        if end_col is None:
            return
        max_cols = self._get_sheet_col_count(sheet)
        logger.info(
            f"追加样式列范围: A{start_row}:{end_col}{end_row} "
            f"(grid_cols={max_cols}, headers={len(headers)})"
        )
        row_style_batch: list[dict] = []
        column_style_batch: list[dict] = []
        header_indices = self._get_header_indices_v2(sheet)

        # 基础数据样式覆盖新增行
        self._append_row_style(
            row_style_batch,
            sheet_id,
            "A",
            end_col,
            start_row,
            end_row,
            STYLE_CONFIG["data"],
        )

        # 分组交替色（序号从 global_group_offset 延续）
        for idx, (s, e) in enumerate(group_ranges):
            global_idx = global_group_offset + idx
            style = (
                STYLE_CONFIG["merged"]
                if global_idx % 2 == 0
                else STYLE_CONFIG["merged_alternate"]
            )
            self._append_row_style(
                row_style_batch,
                sheet_id,
                "A",
                end_col,
                s,
                e,
                style,
            )

        # 设置列样式
        self._append_column_styles(
            column_style_batch,
            sheet,
            header_indices,
            start_row,
            end_row,
        )

        try:
            logger.info("设置行样式...")
            self._apply_style_batches(
                sheet,
                row_style_batch,
                f"追加设置基础样式 行 {start_row}~{end_row}",
            )
            logger.info("设置列样式...")
            self._apply_style_batches(
                sheet,
                column_style_batch,
                f"追加设置列样式 行 {start_row}~{end_row}",
            )
            logger.info("样式设置完成")
        except Exception as e:
            logger.error(f"追加设置样式失败: {e}")
            raise

    def _iter_row_chunks(self, start_row: int, end_row: int, chunk_size: int):
        row = start_row
        while row <= end_row:
            yield row, min(end_row, row + chunk_size - 1)
            row += chunk_size

    def _append_row_style(
        self,
        style_batch: list[dict],
        sheet_id: str,
        start_col: str,
        end_col: str,
        start_row: int,
        end_row: int,
        style: dict,
    ) -> None:
        if start_row > end_row:
            return
        for s, e in self._iter_row_chunks(start_row, end_row, _STYLE_CHUNK_ROWS):
            style_batch.append(
                {
                    "ranges": [f"{sheet_id}!{start_col}{s}:{end_col}{e}"],
                    "style": style,
                }
            )

    def _append_column_styles(
        self,
        style_batch: list[dict],
        sheet: FeishuSheet,
        header_indices: dict[str, int],
        start_row: int,
        end_row: int,
    ) -> None:
        if start_row > end_row:
            return

        for header, cfg in self.config.fields.items():
            col_idx = header_indices.get(header)
            if col_idx is None:
                logger.warning(f"表头 '{header}' 未在 sheet 中找到，跳过 formatter")
                continue

            col_index_1_based = col_idx + 1
            if not self._col_in_grid(sheet, col_index_1_based):
                continue

            col_letter = number_to_letters(col_index_1_based)
            style_cfg = {
                "ranges": [
                    f"{sheet.sheet_id}!{col_letter}{start_row}:{col_letter}{end_row}"
                ],
                "style": {
                    "hAlign": cfg.h_align,
                    "vAlign": cfg.v_align,
                },
            }

            formatter = cfg.formatter
            if formatter:
                style_cfg["style"]["formatter"] = formatter

            if cfg.fore_color:
                style_cfg["style"]["foreColor"] = cfg.fore_color

            style_batch.append(style_cfg)

    def _apply_style_batches(
        self, sheet: FeishuSheet, style_batch: list[dict], desc: str
    ) -> None:
        if not style_batch:
            return
        for i in range(0, len(style_batch), _STYLE_BATCH_SIZE):
            batch = style_batch[i : i + _STYLE_BATCH_SIZE]
            self._retry_on_range_error(
                op=lambda batch=batch: sheet.batch_set_style_v2(batch),
                desc=desc,
                sheet=sheet,
            )

    def _get_sheet_col_count(self, sheet: FeishuSheet) -> int | None:
        if sheet.raw_sheet and sheet.raw_sheet.grid_properties:
            return sheet.raw_sheet.grid_properties.column_count
        return None

    def _get_sheet_row_count(self, sheet: FeishuSheet) -> int | None:
        if sheet.raw_sheet and sheet.raw_sheet.grid_properties:
            return sheet.raw_sheet.grid_properties.row_count
        return None

    def _col_in_grid(self, sheet: FeishuSheet, col_index: int) -> bool:
        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            return False
        return col_index <= max_cols

    def _resolve_end_col_letter(
        self, sheet: FeishuSheet, desired_cols: int
    ) -> str | None:
        if desired_cols < 1:
            return None
        max_cols = self._get_sheet_col_count(sheet)
        if max_cols is None:
            logger.warning("无法获取网格列数，跳过样式设置")
            return None
        desired_cols = min(desired_cols, max_cols)
        if desired_cols < 1:
            return None
        return number_to_letters(desired_cols)

    def _retry_on_range_error(
        self,
        op,
        desc: str,
        sheet: FeishuSheet,
        retries: int = 3,
        wait_sec: int = 1,
    ):
        last_err: Exception | None = None
        for attempt in range(retries):
            try:
                return op()
            except Exception as e:
                last_err = e
                msg = str(e)
                if "90202" not in msg:
                    raise
                if attempt < retries - 1:
                    logger.info(f"{desc} 失败，重试 {attempt + 1}/{retries}")
                    self._refresh_sheet_grid(sheet)
                    sleep(wait_sec)
                else:
                    raise
        if last_err:
            raise last_err

    # ------------------------------------------------------------------
    # Internal — 列宽
    # ------------------------------------------------------------------
    def _set_column_widths(self, sheet: FeishuSheet):
        for idx, (name, cfg) in enumerate(self.config.fields.items()):
            if cfg.width is None:
                continue
            col_index = idx + 1  # 1-based
            try:
                sheet.set_row_col(
                    major_dimension="COLUMNS",
                    start_index=col_index,
                    end_index=col_index,
                    visible=True,
                    fixed_size=cfg.width,
                )
            except Exception as e:
                logger.error(f"设置列宽 '{name}' 失败: {e}")
                logger.error(f"设置列宽 '{name}' 失败: {e}")

    def _find_header_in_sheet(self, sheet: FeishuSheet, header_name: str) -> int | None:
        """
        在 sheet 的表头行中查找指定 header_name，返回列索引（0-based）。未找到返回 None。
        """
        header_row = self._read_header_row(sheet)
        for idx, cell in enumerate(header_row):
            name = str(cell).strip() if cell is not None else ""
            if name == header_name:
                return idx
        return None
