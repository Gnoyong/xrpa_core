"""
Excel to Database Import Tool using SQLAlchemy 2.0
支持字段映射和自定义数据处理功能
"""

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from numbers import Integral, Real
from pathlib import Path
from typing import Any
from xml.etree.ElementTree import ParseError
from zipfile import BadZipFile

import pandas as pd
from pandas import DataFrame
from sqlalchemy import String, select, tuple_
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session

from xrpa_core.core import logger


class SkipRowError(Exception):
    """用于在导入时主动跳过当前行"""


@dataclass
class ExcelImporterConfig:
    """Excel导入配置类，包含字段映射和清洗器等设置"""

    model: type[DeclarativeBase]
    # value支持单列名，或使用"列A|列B"/ ["列A", "列B"] 表示候选列名
    field_mapping: dict[str, str | list[str]]
    field_cleaners: dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    custom_fields_handler: Callable[[pd.Series], dict[str, Any]] | None = None
    sheet_name: str | int | None = None
    start_row: int | None = None
    conflict_keys: list[str] | None = None
    optional_fields: list[str] | None = None


class ExcelImporter:
    """使用SQLAlchemy 2.0的表格数据导入工具类（支持xlsx和csv）"""

    def __init__(self, session: Session):
        """
        初始化导入工具

        Args:
            session: SQLAlchemy Session对象
        """
        self.session = session

    def import_excel(
        self,
        excel_path: str,
        config: ExcelImporterConfig,
        batch_size: int = 500,
        update_on_conflict: bool = False,
    ) -> dict[str, Any]:
        """
        从xlsx或csv导入数据到数据库

        Args:
            excel_path: 文件路径（支持.xlsx和.csv）
            batch_size: 批量插入的大小，默认500
            update_on_conflict: 是否在主键冲突时更新，默认False（跳过）
            config: 导入配置（包含model、字段映射和清洗器等）
        Returns:
            包含导入统计信息的字典
        """
        (
            model,
            resolved_field_mapping,
            field_cleaners,
            custom_fields_handler,
            sheet_name,
            start_row,
            conflict_keys,
            optional_fields,
        ) = self._resolve_import_options(config=config)

        logger.info(
            f"构建数据记录: {excel_path}",
        )

        df, records, errors, row_skipped_count = self._build_records(
            excel_path=excel_path,
            sheet_name=sheet_name,
            start_row=start_row,
            field_mapping=resolved_field_mapping,
            field_cleaners=field_cleaners,
            custom_fields_handler=custom_fields_handler,
            optional_fields=optional_fields,
        )

        # 批量插入数据
        inserted_count = 0
        updated_count = 0
        skipped_count = row_skipped_count

        if records:
            inserted, updated, skipped = self._batch_insert(
                model,
                records,
                batch_size,
                update_on_conflict,
                conflict_keys=conflict_keys,
            )
            inserted_count = inserted
            updated_count = updated
            skipped_count += skipped

        return {
            "total_rows": len(df),
            "success_count": len(records),
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "error_count": len(errors),
            "errors": errors,
        }

    def _resolve_import_options(
        self,
        config: ExcelImporterConfig,
    ) -> tuple[
        type[DeclarativeBase],
        dict[str, str | list[str]],
        dict[str, Callable[[Any], Any]],
        Callable[[pd.Series], dict[str, Any]] | None,
        str | int,
        int,
        list[str] | None,
        list[str] | None,
    ]:
        return (
            config.model,
            config.field_mapping,
            config.field_cleaners,
            config.custom_fields_handler,
            config.sheet_name if config.sheet_name is not None else 0,
            config.start_row if config.start_row is not None else 0,
            config.conflict_keys,
            config.optional_fields,
        )

    def _build_records(
        self,
        excel_path: str,
        sheet_name: str | int,
        start_row: int,
        field_mapping: dict[str, str | list[str]],
        field_cleaners: dict[str, Callable[[Any], Any]],
        custom_fields_handler: Callable[[pd.Series], dict[str, Any]] | None,
        optional_fields: list[str] | None = None,
    ) -> tuple[DataFrame, list[dict[str, Any]], list[dict[str, Any]], int]:
        # 读取表格文件（xlsx/csv）
        df = self._read_tabular_file(
            excel_path, sheet_name=sheet_name, start_row=start_row
        )

        # 空表直接返回：无数据行时无需继续映射和入库
        if df.empty:
            logger.warning("文件无可导入数据，已跳过: {}", excel_path)
            return df, [], [], 0

        # 解析映射关系（支持多个候选列名）
        resolved_field_mapping = self._resolve_field_mapping(
            field_mapping, df.columns, optional_fields=optional_fields
        )

        # 准备导入数据
        records: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        row_skipped_count = 0

        for excel_row_num, (_, row) in enumerate(df.iterrows(), start=2):
            try:
                record: dict[str, Any] = {}
                for excel_col, model_attr, mapping_key in resolved_field_mapping:
                    value = row[excel_col]
                    if pd.isna(value):
                        value = None
                    cleaner = (
                        field_cleaners.get(excel_col)
                        or field_cleaners.get(mapping_key)
                        or field_cleaners.get(model_attr)
                    )
                    if cleaner:
                        value = cleaner(value)
                    record[model_attr] = value

                if custom_fields_handler:
                    custom_fields = custom_fields_handler(row)
                    record.update(custom_fields)

                records.append(record)

            except SkipRowError:
                row_skipped_count += 1
            except RuntimeError as e:
                logger.exception("处理Excel行时发生错误，已跳过: %s", e)
                errors.append({"row": excel_row_num, "error": str(e)})

        return df, records, errors, row_skipped_count

    def _read_tabular_file(
        self,
        file_path: str,
        sheet_name: str | int | None,
        start_row: int,
    ) -> DataFrame:
        """读取xlsx或csv文件并返回DataFrame。"""
        if start_row < 0:
            raise ValueError("start_row must be >= 0")

        suffix = Path(file_path).suffix.lower()

        if suffix == ".csv":
            last_decode_error: UnicodeDecodeError | None = None
            for encoding in ("utf-8-sig", "gbk", "utf-8"):
                try:
                    return pd.read_csv(
                        file_path, skiprows=start_row, encoding=encoding, dtype=str
                    )
                except UnicodeDecodeError as e:
                    last_decode_error = e
                    continue
            raise RuntimeError(
                f"读取 CSV 失败，无法识别编码: {file_path}"
            ) from last_decode_error

        if suffix == ".xlsx":
            excel_sheet_name: str | int = (
                sheet_name if sheet_name is not None else "Sheet1"
            )
            for attempt in range(3):
                try:
                    return pd.read_excel(
                        file_path,
                        sheet_name=excel_sheet_name,
                        skiprows=start_row,
                        dtype=str,
                    )
                except (ParseError, BadZipFile) as e:
                    if attempt < 2:
                        logger.warning(
                            "读取 Excel 失败，准备重试 ({}/3): {} | 错误: {}",
                            attempt + 1,
                            file_path,
                            e,
                        )
                        time.sleep(1)
                        continue
                    raise RuntimeError(
                        f"读取 Excel 失败，文件可能损坏: {file_path}"
                    ) from e

        raise ValueError(
            f"不支持的文件类型: {suffix or '无后缀'}，仅支持 .csv 和 .xlsx"
        )

    def _resolve_field_mapping(
        self,
        field_mapping: dict[str, str | list[str]],
        available_columns: Any,
        optional_fields: list[str] | None = None,
    ) -> list[tuple[str, str, str]]:
        """将字段映射解析为实际可用列名，支持多个候选列名。"""
        available_set = set(available_columns)
        normalized_lookup: dict[str, str] = {}
        for original_col in available_columns:
            normalized_col = self._normalize_column_name(original_col)
            if not normalized_col:
                continue
            if (
                normalized_col in normalized_lookup
                and normalized_lookup[normalized_col] != original_col
            ):
                logger.warning(
                    "检测到规范化后重复列名: '{}' 和 '{}'，将优先使用前者",
                    normalized_lookup[normalized_col],
                    original_col,
                )
                continue
            normalized_lookup[normalized_col] = str(original_col)

        resolved: list[tuple[str, str, str]] = []
        missing_groups: list[str] = []

        for model_attr, source_cols in field_mapping.items():
            candidates, mapping_key = self._parse_mapping_candidates(source_cols)

            # 先精确匹配，再做标准化匹配（忽略大小写/多空格/BOM/不间断空格）
            matched_col = next(
                (col for col in candidates if col in available_set), None
            )
            if matched_col is None:
                matched_col = next(
                    (
                        normalized_lookup.get(self._normalize_column_name(col))
                        for col in candidates
                        if self._normalize_column_name(col) in normalized_lookup
                    ),
                    None,
                )

            if matched_col is None:
                if optional_fields and model_attr in optional_fields:
                    continue
                missing_groups.append(f"[{model_attr}] -> [{mapping_key}]")
                continue

            resolved.append((matched_col, model_attr, mapping_key))

        if missing_groups:
            formated = "\n".join(f"  - {group}" for group in missing_groups)

            raise ValueError("Excel中缺少以下列（每组至少命中一个）:\n" + formated)

        return resolved

    def _parse_mapping_candidates(
        self, source_cols: str | list[str]
    ) -> tuple[list[str], str]:
        """解析映射key为候选列名列表和展示用key。"""
        if isinstance(source_cols, str):
            candidates = [part.strip() for part in source_cols.split("|")]
        elif isinstance(source_cols, list):
            candidates = [str(part).strip() for part in source_cols]

        candidates = [col for col in candidates if col]
        if not candidates:
            raise ValueError("field_mapping 中存在空列名配置")

        mapping_key = " | ".join(candidates)
        return candidates, mapping_key

    @staticmethod
    def _normalize_column_name(value: Any) -> str:
        """标准化列名，减少因大小写、空白字符、BOM差异导致的匹配失败。"""
        text = str(value)
        text = text.replace("\ufeff", "").replace("\xa0", " ")
        text = " ".join(text.split())
        return text.strip().casefold()

    def _batch_insert(
        self,
        model: type[DeclarativeBase],
        records: list[dict],
        batch_size: int,
        update_on_conflict: bool = False,
        conflict_keys: list[str] | None = None,
    ) -> tuple[int, int, int]:
        """
        批量插入数据

        Args:
            model: SQLAlchemy模型类
            records: 记录列表
            batch_size: 批量大小
            update_on_conflict: 是否在冲突时更新
            conflict_keys: 冲突键列表（默认主键）

        Returns:
            (插入数量, 更新数量, 跳过数量)
        """
        if not records:
            return 0, 0, 0

        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        unique_warned = False

        mapper = model.__mapper__
        pk_columns = [col.name for col in mapper.primary_key]
        key_columns = conflict_keys or [col.name for col in mapper.primary_key]
        if update_on_conflict and key_columns:
            invalid_keys = [k for k in key_columns if k not in model.__table__.columns]
            if invalid_keys:
                raise ValueError(f"conflict_keys 包含无效字段: {invalid_keys}")

        def _normalize_lookup_value(column_name: str, value: Any) -> Any:
            if value is None:
                return None
            column = model.__table__.columns.get(column_name)
            if column is None:
                return value
            if isinstance(column.type, String):
                if isinstance(value, Integral):
                    return str(int(value))
                if isinstance(value, Real) and not isinstance(value, Integral):
                    numeric = float(value)
                    if numeric.is_integer():
                        return str(int(numeric))
                    return format(numeric, ".15g")
            return value

        def _build_key_values(record: dict[str, Any]) -> dict[str, Any] | None:
            key_values = {
                k: _normalize_lookup_value(k, record.get(k))
                for k in key_columns
                if k in record
            }
            if not key_values or any(v is None for v in key_values.values()):
                return None
            return key_values

        def _query_existing_pk_map(
            key_tuples: list[tuple[Any, ...]],
        ) -> dict[tuple[Any, ...], tuple[Any, ...]]:
            if not key_columns or not key_tuples:
                return {}

            unique_key_tuples = list(dict.fromkeys(key_tuples))
            key_column_objs = [model.__table__.columns[k] for k in key_columns]
            pk_column_objs = [model.__table__.columns[k] for k in pk_columns]

            if len(key_column_objs) == 1:
                stmt = select(*pk_column_objs, key_column_objs[0]).where(
                    key_column_objs[0].in_([key[0] for key in unique_key_tuples])
                )
            else:
                stmt = select(*pk_column_objs, *key_column_objs).where(
                    tuple_(*key_column_objs).in_(unique_key_tuples)
                )

            rows = self.session.execute(stmt).all()
            key_to_pk: dict[tuple[Any, ...], tuple[Any, ...]] = {}
            pk_len = len(pk_column_objs)
            for row in rows:
                row_values = tuple(row)
                pk_tuple = row_values[:pk_len]
                key_tuple = row_values[pk_len:]
                key_to_pk[key_tuple] = pk_tuple

            return key_to_pk

        def _insert_batch_rowwise(batch: list[dict]) -> tuple[int, int, int]:
            nonlocal unique_warned

            batch_inserted = 0
            batch_updated = 0
            batch_skipped = 0

            if update_on_conflict:
                for record in batch:
                    key_values = _build_key_values(record)

                    if key_values:
                        stmt = select(model).filter_by(**key_values)
                        existing = self.session.execute(stmt).scalar_one_or_none()

                        if existing:
                            for key, value in record.items():
                                setattr(existing, key, value)
                            batch_updated += 1
                        else:
                            self.session.add(model(**record))
                            batch_inserted += 1
                    else:
                        self.session.add(model(**record))
                        batch_inserted += 1

                return batch_inserted, batch_updated, batch_skipped

            for record in batch:
                try:
                    with self.session.begin_nested():
                        self.session.add(model(**record))
                        self.session.flush()
                    batch_inserted += 1
                except IntegrityError as e:
                    batch_skipped += 1
                    if not unique_warned:
                        msg = str(e).lower()
                        if "unique" in msg:
                            logger.warning("检测到唯一约束冲突，后续重复记录将被跳过")
                            unique_warned = True
                        else:
                            logger.warning(
                                "插入记录时发生完整性错误，已跳过，错误: {}", e
                            )
                except (SQLAlchemyError, RuntimeError, ValueError, TypeError) as e:
                    logger.exception(f"插入记录时发生错误，已跳过: {e}")
                    batch_skipped += 1

            return batch_inserted, batch_updated, batch_skipped

        # 分批处理
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            batch_inserted = 0
            batch_updated = 0
            batch_skipped = 0

            if update_on_conflict:
                try:
                    key_records: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
                    records_without_key: list[dict[str, Any]] = []

                    for record in batch:
                        key_values = _build_key_values(record)
                        if key_values is None:
                            records_without_key.append(record)
                            continue
                        key_records.append(
                            (tuple(key_values[k] for k in key_columns), record)
                        )

                    existing_pk_map = _query_existing_pk_map(
                        [key_tuple for key_tuple, _ in key_records]
                    )

                    update_mappings: list[dict[str, Any]] = []
                    insert_records: list[dict[str, Any]] = []
                    insert_index_by_key: dict[tuple[Any, ...], int] = {}

                    for key_tuple, record in key_records:
                        existing_pk = existing_pk_map.get(key_tuple)
                        if existing_pk is not None:
                            mapping = dict(record)
                            for idx, pk_col in enumerate(pk_columns):
                                mapping[pk_col] = existing_pk[idx]
                            update_mappings.append(mapping)
                            continue

                        previous_index = insert_index_by_key.get(key_tuple)
                        if previous_index is not None:
                            insert_records[previous_index] = record
                            batch_updated += 1
                            continue

                        insert_index_by_key[key_tuple] = len(insert_records)
                        insert_records.append(record)

                    insert_records.extend(records_without_key)

                    if update_mappings:
                        self.session.bulk_update_mappings(mapper, update_mappings)
                    if insert_records:
                        self.session.bulk_insert_mappings(mapper, insert_records)

                    batch_inserted += len(insert_records)
                    batch_updated += len(update_mappings)
                except IntegrityError as e:
                    self.session.rollback()
                    logger.warning("bulk upsert失败，已回退到逐行处理: {}", e)
                    (
                        batch_inserted,
                        batch_updated,
                        batch_skipped,
                    ) = _insert_batch_rowwise(batch)
                except (SQLAlchemyError, RuntimeError, ValueError, TypeError) as e:
                    self.session.rollback()
                    logger.warning("bulk upsert失败，已回退到逐行处理: {}", e)
                    (
                        batch_inserted,
                        batch_updated,
                        batch_skipped,
                    ) = _insert_batch_rowwise(batch)
            else:
                try:
                    self.session.bulk_insert_mappings(mapper, batch)
                    batch_inserted = len(batch)
                except IntegrityError:
                    self.session.rollback()
                    (
                        batch_inserted,
                        batch_updated,
                        batch_skipped,
                    ) = _insert_batch_rowwise(batch)
                except (SQLAlchemyError, RuntimeError, ValueError, TypeError) as e:
                    self.session.rollback()
                    logger.warning("bulk insert失败，已回退到逐行处理: {}", e)
                    (
                        batch_inserted,
                        batch_updated,
                        batch_skipped,
                    ) = _insert_batch_rowwise(batch)

            # 提交批次
            try:
                self.session.commit()
            except Exception as e:
                self.session.rollback()
                raise e

            logger.info(
                "批次处理完成: {}~{} ( 插入: {} | 更新: {} | 跳过: {} )",
                i + 1,
                min(i + batch_size, len(records)),
                batch_inserted,
                batch_updated,
                batch_skipped,
            )

            inserted_count += batch_inserted
            updated_count += batch_updated
            skipped_count += batch_skipped

        return inserted_count, updated_count, skipped_count
