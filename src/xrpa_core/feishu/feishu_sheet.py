import json
import re
from typing import Literal

import lark_oapi as lark
from lark_oapi.api.sheets.v3 import (
    Find,
    FindCondition,
    FindSpreadsheetSheetRequest,
    FindSpreadsheetSheetResponse,
    GetSpreadsheetRequest,
    GetSpreadsheetResponse,
    GetSpreadsheetSheetRequest,
    GetSpreadsheetSheetResponse,
    QuerySpreadsheetSheetRequest,
    QuerySpreadsheetSheetResponse,
)
from lark_oapi.api.sheets.v3.model import (
    FindSpreadsheetSheetResponseBody,
    GetSpreadsheetSheetResponseBody,
    QuerySpreadsheetSheetResponseBody,
    Sheet,
)
from pydantic import BaseModel, Field

from xrpa_core.core.logger import logger
from xrpa_core.feishu.feishu_api_base import (
    FeishuApiBase,
    ResponseError,
    _parse_json_response,
)

from .excel_utils import letters_to_number, number_to_letters

# ==========================================================
# Exception
# ==========================================================


class GetDetailFaildException(Exception):
    pass


class SheetExistedException(Exception):
    pass


# ==========================================================
# Data Model
# ==========================================================


class SheetProtectModel(BaseModel):
    lock: Literal["LOCK", "UNLOCK"] = Field(
        ...,
        description="是否要保护该工作表",
    )
    lockInfo: str | None = None
    userIDs: list[str] | None = None


class UpdateSheetRequestModel(BaseModel):
    sheetId: str

    title: str | None = None
    index: int | None = None
    hidden: bool | None = None
    frozenRowCount: int | None = None
    frozenColCount: int | None = None
    protect: SheetProtectModel | None = None


class ProtectedDimensionModel(BaseModel):
    sheetId: str | None = None
    majorDimension: Literal["ROWS", "COLUMNS"] = "ROWS"
    startIndex: int
    endIndex: int


class AddProtectedDimensionRequestModel(BaseModel):
    dimension: ProtectedDimensionModel
    editors: list[int] | None = None
    users: list[str] | None = None
    lockInfo: str | None = None


class StyleFontModel(BaseModel):
    bold: bool | None = None
    italic: bool | None = None
    fontSize: str | None = None
    clean: bool | None = None


class BatchCellStyleModel(BaseModel):
    font: StyleFontModel | None = None
    textDecoration: Literal[0, 1, 2, 3] | None = None
    formatter: str | None = None
    hAlign: Literal[0, 1, 2] | None = None
    vAlign: Literal[0, 1, 2] | None = None
    foreColor: str | None = None
    backColor: str | None = None
    borderType: (
        Literal[
            "FULL_BORDER",
            "OUTER_BORDER",
            "INNER_BORDER",
            "NO_BORDER",
            "LEFT_BORDER",
            "RIGHT_BORDER",
            "TOP_BORDER",
            "BOTTOM_BORDER",
        ]
        | None
    ) = None
    borderColor: str | None = None
    clean: bool | None = None


class BatchSetStyleDataModel(BaseModel):
    ranges: list[str]
    style: BatchCellStyleModel


# ==========================================================
# Feishu Sheet
# ==========================================================


class FeishuSheet(FeishuApiBase):
    """
    飞书工作表对象，封装了对单个工作表的操作
    """

    def __init__(
        self,
        client,
        spreadsheet_token: str,
        sheet_id: str,
        raw_sheet: Sheet | None = None,
    ):
        super().__init__(client)
        self.spreadsheet_token = spreadsheet_token
        self.sheet_id = sheet_id

        # 由 SpreadSheet 赋值
        if raw_sheet:
            self.raw_sheet: Sheet = raw_sheet
        else:
            detail = self._get_detail()
            if detail is None or detail.sheet is None:
                raise GetDetailFaildException("获取 Sheet 详情失败")
            self.raw_sheet = detail.sheet

        if self.raw_sheet.sheet_id is None:
            raise ValueError("raw_sheet 中 sheet_id 不能为空")

        self.id = self.raw_sheet.sheet_id
        self.title = self.raw_sheet.title

    def get_id(self):
        return self.id

    def get_title(self):
        return self.title

    def _get_detail(self) -> GetSpreadsheetSheetResponseBody | None:
        request: GetSpreadsheetSheetRequest = (
            GetSpreadsheetSheetRequest.builder()
            .spreadsheet_token(self.spreadsheet_token)
            .sheet_id(self.sheet_id)
            .build()
        )

        response: GetSpreadsheetSheetResponse = self._invoke_response(
            f"获取工作表详情 {self.sheet_id}",
            lambda: self.client.sheets.v3.spreadsheet_sheet.get(request),
        )

        # 处理业务结果
        return response.data

    def refresh_raw_sheet(self) -> Sheet | None:
        detail = self._get_detail()
        if detail is None:
            return None

        sheet = detail.sheet
        if sheet is not None:
            self.raw_sheet = sheet

        return self.raw_sheet

    def delete_dimension_range(
        self, major_dimension: str, start_index: int, end_index: int
    ):
        """
        delete_dimension_range 的 Docstring

        :param self: 说明
        :param major_dimension: 说明
        :type major_dimension: str
        :param start_index: 说明
        :type start_index: int
        :param end_index: 说明
        :type end_index: int
        """
        response = self._request(
            lark.HttpMethod.DELETE,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/dimension_range",
            body={
                "dimension": {
                    "sheetId": self.sheet_id,
                    "majorDimension": major_dimension,
                    "startIndex": start_index,
                    "endIndex": end_index,
                }
            },
        )
        return _parse_json_response(response)

    def write_range(self, sells_range: str, values: list[list | None]):
        """
        向电子表格某个工作表的单个指定范围中写入数据。若指定范围内已有数据，将被新写入的数据覆盖。
        使用限制
        单次写入数据不得超过 5000 行、100列。
        每个单元格不超过 50,000 字符，由于服务端会增加控制字符，因此推荐每个单元格不超过 40,000 字符。

        :param sells_range: 说明
        :type sells_range: str
        :param values: 说明
        :type values: List[List]
        """
        max_cols = (
            max(
                (
                    len(row)
                    if isinstance(row, (list, tuple))
                    else 0
                    if row is None
                    else 1
                )
                for row in values
            )
            if values
            else 0
        )
        if max_cols > 100:
            raise ValueError(f"单次写入列数不得超过 100，当前为 {max_cols}")

        if len(values) <= 5000:
            return self._request_json(
                lark.HttpMethod.PUT,
                "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/values",
                body={
                    "valueRange": {
                        "range": f"{self.sheet_id}!{sells_range}",
                        "values": values,
                    }
                },
            )

        chunks = self._split_write_chunks(sells_range, values)
        logger.info(
            f"写入范围 {sells_range} 共 {len(values)} 行，自动拆分为 {len(chunks)} 批请求"
        )

        last_result: dict = {}
        for index, (chunk_range, chunk_values) in enumerate(chunks, start=1):
            logger.info(
                f"写入数据批次 {index}/{len(chunks)}：{self.sheet_id}!{chunk_range}"
            )
            last_result = self._request_json(
                lark.HttpMethod.PUT,
                "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/values",
                body={
                    "valueRange": {
                        "range": f"{self.sheet_id}!{chunk_range}",
                        "values": chunk_values,
                    }
                },
            )

        return last_result

    def _operation(self, operation: dict):
        return self._request_json(
            lark.HttpMethod.POST,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/sheets_batch_update",
            body={"requests": [operation]},
        )

    def copy(self, name: str):
        """
        copy 的 Docstring

        :param self: 说明
        :param name: 说明
        :type name: str
        """
        self._operation(
            {
                "copySheet": {
                    "source": {"sheetId": self.sheet_id},
                    "destination": {"title": name},
                }
            }
        )

    def search(
        self,
        cell_range: str,
        find: str,
        match_case: bool = False,
        match_entire_cell: bool = False,
        search_by_regex: bool = False,
        include_formulas: bool = False,
    ) -> FindSpreadsheetSheetResponseBody | None:
        """
        搜索单元格

        :param self: 说明
        """
        # 构造请求对象
        request: FindSpreadsheetSheetRequest = (
            FindSpreadsheetSheetRequest.builder()
            .spreadsheet_token(self.spreadsheet_token)
            .sheet_id(self.sheet_id)
            .request_body(
                Find.builder()
                .find_condition(
                    FindCondition.builder()
                    .range(f"{self.sheet_id}!{cell_range}")
                    .match_case(match_case)
                    .match_entire_cell(match_entire_cell)
                    .search_by_regex(search_by_regex)
                    .include_formulas(include_formulas)
                    .build()
                )
                .find(find)
                .build()
            )
            .build()
        )

        # 发起请求
        response: FindSpreadsheetSheetResponse = self._invoke_response(
            f"搜索单元格 {self.sheet_id}",
            lambda: self.client.sheets.v3.spreadsheet_sheet.find(request),
        )

        # 处理业务结果
        return response.data

    def search_v2(
        self,
        find: str,
        match_case: bool = False,
        match_entire_cell: bool = False,
        search_by_regex: bool = False,
        include_formulas: bool = False,
    ) -> FindSpreadsheetSheetResponseBody | None:
        """
        搜索单元格

        :param self: 说明
        """
        if not self.raw_sheet:
            raise ValueError("raw_sheet 未初始化，无法获取行列数")

        if not self.raw_sheet.grid_properties:
            raise ValueError("缺少 grid_properties 信息，无法获取行列数")

        row_count = self.raw_sheet.grid_properties.row_count
        col_count = self.raw_sheet.grid_properties.column_count

        if row_count is None or col_count is None:
            raise ValueError("行列数信息不完整，无法获取行列数")

        if row_count > 5000 or col_count > 5000:
            raise ValueError("行数超过 5000 行了")

        cell_range = f"A1:{number_to_letters(row_count)}{col_count}"
        return self.search(
            cell_range,
            find,
            match_case,
            match_entire_cell,
            search_by_regex,
            include_formulas,
        )

    def get_range(self, cell_range: str):
        """
        get_range 的 Docstring

        :param self: 说明
        """
        return self._request_json(
            lark.HttpMethod.GET,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/values/:range",
            paths={
                "spreadsheetToken": self.spreadsheet_token,
                "range": f"{self.sheet_id}!{cell_range}",
            },
        )

    def get_range_v2(self, cell_range: str):
        """
        get_range 的 Docstring

        :param self: 说明
        """
        res = self.get_range(cell_range)
        data = res.get("data")
        if data is None:
            return None
        value_range = data.get("valueRange")
        if value_range is None:
            return None
        return value_range.get("values")

    def set_data_validation(self, cell_range, condition_values, options):
        """
        {
            "multipleValues": True,
            "highlightValidData": True,
            "colors": ["#1FB6C1", "#F006C2", "#FB16C3", "#FFB6C1"]
        }
        """
        body = {
            "range": f"{self.sheet_id}!{cell_range}",
            "dataValidationType": "list",
            "dataValidation": {"conditionValues": condition_values, "options": options},
        }

        return self._request_json(
            lark.HttpMethod.POST,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/dataValidation",
            body=body,
        )

    def merge_cells(self, cell_range: str, merge_type: str):
        body = {"range": f"{self.sheet_id}!{cell_range}", "mergeType": merge_type}
        return self._request_json(
            lark.HttpMethod.POST,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/merge_cells",
            body=body,
        )

    # def batch_set_style(self, style_config):
    #     api = "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/styles_batch_update"
    #     body = {"data": style_config}
    #     return self._request_json(lark.HttpMethod.PUT, api, body=body)

    def batch_set_style_v2(
        self, style_data: list[BatchSetStyleDataModel | dict]
    ) -> dict:
        """
        使用 Pydantic 模型规范化请求并批量设置单元格样式。
        当设置边框时，若单元格数量超过 30000，会自动拆分为多次请求。

        :param style_data: 支持 BatchSetStyleDataModel 或 dict
        """
        if not style_data:
            raise ValueError("style_data 不能为空")

        # 解析并规范化所有样式项
        parsed_items: list[
            tuple[list[str], dict, int]
        ] = []  # (ranges, style_dict, border_cells)
        total_border_cells = 0

        for raw_item in style_data:
            item = (
                raw_item
                if isinstance(raw_item, BatchSetStyleDataModel)
                else BatchSetStyleDataModel.model_validate(raw_item)
            )

            if len(item.ranges) == 0:
                raise ValueError("ranges 不能为空")

            normalized_ranges: list[str] = []
            item_border_cells = 0

            for range_text in item.ranges:
                full_range = (
                    range_text if "!" in range_text else f"{self.sheet_id}!{range_text}"
                )
                normalized_ranges.append(full_range)

                size = self._parse_a1_range_size(full_range)
                if size is None:
                    continue

                rows, cols = size
                if rows > 5000:
                    raise ValueError(
                        f"单个范围行数不能超过 5000，当前 {full_range} 行数为 {rows}"
                    )
                if cols > 100:
                    raise ValueError(
                        f"单个范围列数不能超过 100，当前 {full_range} 列数为 {cols}"
                    )

                if item.style.borderType and item.style.borderType != "NO_BORDER":
                    item_border_cells += rows * cols

            style_dict = item.style.model_dump(exclude_none=True)
            parsed_items.append((normalized_ranges, style_dict, item_border_cells))
            total_border_cells += item_border_cells

        # 如果没有边框或边框单元格数未超限，直接发送单次请求
        if total_border_cells == 0 or total_border_cells <= 30000:
            request_data = [
                {"ranges": ranges, "style": style_dict}
                for ranges, style_dict, _ in parsed_items
            ]
            return self._request_json(
                lark.HttpMethod.PUT,
                "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/styles_batch_update",
                body={"data": request_data},
                action_name=f"批量设置样式 {self.sheet_id}",
            )

        # 边框单元格超限时，拆分为多个批次
        logger.info(
            f"边框单元格数量 {total_border_cells} 超过 30000 限制，自动拆分请求"
        )
        batches: list[list[dict]] = []
        current_batch: list[dict] = []
        current_border_cells = 0

        for ranges, style_dict, item_border_cells in parsed_items:
            # 如果单个项就超过限制，需要进一步拆分 ranges
            if item_border_cells > 30000:
                # 先提交当前批次
                if current_batch:
                    batches.append(current_batch)
                    current_batch = []
                    current_border_cells = 0

                # 拆分该 item 的 ranges
                for range_text in ranges:
                    size = self._parse_a1_range_size(range_text)
                    if size is None:
                        # 无法解析的范围，单独作为一个请求
                        if current_batch:
                            batches.append(current_batch)
                            current_batch = []
                            current_border_cells = 0
                        batches.append([{"ranges": [range_text], "style": style_dict}])
                        continue

                    rows, cols = size
                    range_border_cells = (
                        rows * cols
                        if style_dict.get("borderType")
                        and style_dict.get("borderType") != "NO_BORDER"
                        else 0
                    )

                    if range_border_cells > 30000:
                        # 单个范围就超限，需要按行拆分
                        # 解析范围获取起始行列
                        target = range_text.split("!", 1)[-1]
                        span_re = re.compile(r"^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$")
                        span = span_re.fullmatch(target)
                        if not span:
                            # 无法解析，直接提交
                            batches.append(
                                [{"ranges": [range_text], "style": style_dict}]
                            )
                            continue

                        start_col, start_row_str, end_col, end_row_str = span.groups()
                        start_row = int(start_row_str)
                        end_row = int(end_row_str)

                        # 按行拆分，确保每批不超过 30000 单元格
                        chunk_start = start_row
                        while chunk_start <= end_row:
                            # 计算每批最大行数
                            max_rows_per_chunk = max(1, 30000 // cols)
                            chunk_end = min(
                                chunk_start + max_rows_per_chunk - 1, end_row
                            )
                            chunk_range = (
                                f"{start_col}{chunk_start}:{end_col}{chunk_end}"
                            )
                            batches.append(
                                [{"ranges": [chunk_range], "style": style_dict}]
                            )
                            chunk_start = chunk_end + 1
                    else:
                        # 单个范围未超限，加入当前批次
                        if current_border_cells + range_border_cells > 30000:
                            batches.append(current_batch)
                            current_batch = []
                            current_border_cells = 0
                        current_batch.append(
                            {"ranges": [range_text], "style": style_dict}
                        )
                        current_border_cells += range_border_cells
            else:
                # 检查加入当前项是否会超限
                if current_border_cells + item_border_cells > 30000:
                    batches.append(current_batch)
                    current_batch = []
                    current_border_cells = 0

                current_batch.append({"ranges": ranges, "style": style_dict})
                current_border_cells += item_border_cells

        # 提交最后一批
        if current_batch:
            batches.append(current_batch)

        # 发送所有批次
        last_result: dict = {}
        for i, batch in enumerate(batches):
            logger.debug(
                f"发送样式批次 {i + 1}/{len(batches)}，包含 {len(batch)} 个范围"
            )
            last_result = self._request_json(
                lark.HttpMethod.PUT,
                "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/styles_batch_update",
                body={"data": batch},
                action_name=f"批量设置样式 {self.sheet_id} (批次 {i + 1}/{len(batches)})",
            )

        return last_result

    def set_row_col(
        self, major_dimension: str, start_index, end_index, visible, fixed_size
    ):
        api = "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/dimension_range"
        body = {
            "dimension": {
                "sheetId": self.sheet_id,
                "majorDimension": major_dimension,
                "startIndex": start_index,
                "endIndex": end_index,
            },
            "dimensionProperties": {"visible": visible, "fixedSize": fixed_size},
        }
        return self._request_json(lark.HttpMethod.PUT, api, body=body)

    def protected_dimension(
        self,
        add_protected_dimension: list[AddProtectedDimensionRequestModel | dict],
        user_id_type: Literal["open_id", "union_id"] | None = None,
    ) -> dict:
        """
        批量为当前工作表设置行/列保护范围。

        :param add_protected_dimension: 保护范围配置，支持传入 dict 或 AddProtectedDimensionRequestModel
        :param user_id_type: 当传入 users 字段时，必须指定 open_id 或 union_id
        """
        if len(add_protected_dimension) == 0:
            raise ValueError("add_protected_dimension 不能为空")

        if len(add_protected_dimension) > 50:
            raise ValueError("最多支持传入 50 个维度信息")

        request_items: list[dict] = []
        protected_count = 0

        for raw_item in add_protected_dimension:
            item = (
                raw_item
                if isinstance(raw_item, AddProtectedDimensionRequestModel)
                else AddProtectedDimensionRequestModel.model_validate(raw_item)
            )
            dimension = item.dimension
            start_index = dimension.startIndex
            end_index = dimension.endIndex

            if start_index < 1 or end_index < 1:
                raise ValueError("startIndex 和 endIndex 必须从 1 开始")

            if end_index < start_index:
                raise ValueError("endIndex 不能小于 startIndex")

            if item.users and user_id_type is None:
                raise ValueError("传入 users 时必须指定 user_id_type")

            protected_count += end_index - start_index + 1

            request_item = item.model_dump(exclude_none=True)
            request_item["dimension"]["sheetId"] = (
                dimension.sheetId if dimension.sheetId else self.sheet_id
            )
            request_items.append(request_item)

        if protected_count > 5000:
            raise ValueError("单次调用最多支持保护 5000 行或列")

        query_params = None
        if user_id_type:
            query_params = {"user_id_type": user_id_type}

        return self._request_json(
            lark.HttpMethod.POST,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/protected_dimension",
            body={"addProtectedDimension": request_items},
            query_params=query_params,
            action_name=f"设置保护范围 {self.sheet_id}",
        )

    @staticmethod
    def _split_write_chunks(
        sells_range: str,
        values: list[list | None],
        max_rows_per_request: int = 5000,
    ) -> list[tuple[str, list[list | None]]]:
        """
        将写入数据按行拆分为多个 A1 范围批次，避免超出单次 5000 行限制。
        """
        if max_rows_per_request < 1:
            raise ValueError("max_rows_per_request 必须大于 0")

        if not values:
            return [(sells_range, values)]

        target = sells_range.split("!", 1)[-1]
        single_re = re.compile(r"^([A-Za-z]+)(\d+)$")
        span_re = re.compile(r"^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$")

        single = single_re.fullmatch(target)
        if single:
            start_col, start_row_str = single.groups()
            start_row = int(start_row_str)
            max_cols = (
                max(
                    (
                        len(row)
                        if isinstance(row, (list, tuple))
                        else 0
                        if row is None
                        else 1
                    )
                    for row in values
                )
                or 1
            )
            end_col = number_to_letters(letters_to_number(start_col) + max_cols - 1)
        else:
            span = span_re.fullmatch(target)
            if not span:
                raise ValueError(
                    f"超过 5000 行时，sells_range 必须是标准 A1 格式: {sells_range}"
                )
            start_col, start_row_str, end_col, _ = span.groups()
            start_row = int(start_row_str)

        chunks: list[tuple[str, list[list | None]]] = []
        for offset in range(0, len(values), max_rows_per_request):
            chunk_values = values[offset : offset + max_rows_per_request]
            chunk_start_row = start_row + offset
            chunk_end_row = chunk_start_row + len(chunk_values) - 1
            chunk_range = f"{start_col}{chunk_start_row}:{end_col}{chunk_end_row}"
            chunks.append((chunk_range, chunk_values))

        return chunks

    @staticmethod
    def _parse_a1_range_size(range_text: str) -> tuple[int, int] | None:
        """
        解析标准 A1 范围，返回 (rows, cols)。
        对不符合 A1 格式的范围返回 None（交由服务端校验）。
        """
        target = range_text.split("!", 1)[-1]
        single_re = re.compile(r"^([A-Za-z]+)(\d+)$")
        span_re = re.compile(r"^([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)$")

        single = single_re.fullmatch(target)
        if single:
            return (1, 1)

        span = span_re.fullmatch(target)
        if not span:
            return None

        start_col, start_row, end_col, end_row = span.groups()
        start_row_int = int(start_row)
        end_row_int = int(end_row)
        if start_row_int < 1 or end_row_int < 1:
            raise ValueError(f"范围行号必须从 1 开始: {range_text}")

        start_col_int = letters_to_number(start_col)
        end_col_int = letters_to_number(end_col)
        rows = abs(end_row_int - start_row_int) + 1
        cols = abs(end_col_int - start_col_int) + 1
        return (rows, cols)


class FeishuSpreadSheet(FeishuApiBase):
    """
    飞书电子表格对象，封装了对电子表格的操作，包括获取表格信息、添加/删除工作表等
    """

    def __init__(self, client, spreadsheet_token: str):
        if not spreadsheet_token or len(spreadsheet_token) == 0:
            raise ValueError("必须提供有效的 spreadsheet_token")
        super().__init__(client, spreadsheet_token)

        raw_sheets = self._get_sheets().sheets
        if raw_sheets is None:
            raise ValueError("获取的 sheets 列表为空，无法初始化 FeishuSpreadSheet")

        self.sheets: list[FeishuSheet] = []
        for raw_sheet in raw_sheets:
            if raw_sheet.sheet_id is None:
                logger.warning(f"跳过 sheet_id 为空的 sheet，sheet 信息：{raw_sheet}")
                continue
            sheet = FeishuSheet(
                self.client, self.spreadsheet_token, raw_sheet.sheet_id, raw_sheet
            )
            self.sheets.append(sheet)

    def _get_sheets(self) -> QuerySpreadsheetSheetResponseBody:
        request: QuerySpreadsheetSheetRequest = (
            QuerySpreadsheetSheetRequest.builder()
            .spreadsheet_token(self.spreadsheet_token)
            .build()
        )

        # 发起请求
        response: QuerySpreadsheetSheetResponse = self._invoke_response(
            "获取工作表列表",
            lambda: self.client.sheets.v3.spreadsheet_sheet.query(request),
        )

        # 处理业务结果
        data = response.data
        if not isinstance(data, QuerySpreadsheetSheetResponseBody):
            raise ResponseError("响应体格式不正确，缺少 sheets 字段")
        return data

    def get_info(self):
        # 构造请求对象
        request: GetSpreadsheetRequest = (
            GetSpreadsheetRequest.builder()
            .spreadsheet_token(self.spreadsheet_token)
            .build()
        )

        # 发起请求
        response: GetSpreadsheetResponse = self._invoke_response(
            "获取电子表格信息",
            lambda: self.client.sheets.v3.spreadsheet.get(request),
        )

        # 处理业务结果
        return _parse_json_response(response)

    def find_sheet_by_id(self, sheet_id: str):
        """
        通过id找表格

        :param self: 说明
        :param sheet_id: 说明
        :type sheet_id: str
        :return: 说明
        :rtype: Any
        """
        results = [sheet for sheet in self.sheets if sheet.sheet_id == sheet_id]
        if len(results) == 0:
            return None
        else:
            return results[0]

    def find_sheet_by_name(self, name: str):
        """
        通过name找表格

        :param self: 说明
        :param sheet_id: 说明
        :type sheet_id: str
        :return: 说明
        :rtype: Any
        """
        results = [sheet for sheet in self.sheets if sheet.raw_sheet.title == name]
        if len(results) == 0:
            return None
        else:
            return results[0]

    def _operation(self, operations: list[dict]):
        json_data = self._request_json(
            lark.HttpMethod.POST,
            "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/sheets_batch_update",
            body={"requests": operations},
        )
        return json_data

    def _replies_to_title_dict(self, resp: dict) -> dict:
        return {
            item["addSheet"]["properties"]["title"]: item["addSheet"]["properties"]
            for item in resp.get("data", {}).get("replies", [])
            if "addSheet" in item and "properties" in item["addSheet"]
        }

    def add_sheet(self, sheet_name, index: int = 0):
        """
        return sheet_to_sheet_id
        """
        try:
            result = self._operation(
                [{"addSheet": {"properties": {"title": sheet_name, "index": index}}}],
            )
        except ResponseError as e:
            if e.response and isinstance(e.response, dict):
                error_code = e.response.get("code")
                if error_code == 90210:
                    raise SheetExistedException(f"工作表 '{sheet_name}' 已存在") from e
            raise
        return self._replies_to_title_dict(result)

    def add_sheets(self, sheet_names: list[str]):
        """
        return sheet_to_sheet_id
        """
        result = self._operation(
            [
                {"addSheet": {"properties": {"title": sheet_name, "index": 0}}}
                for sheet_name in sheet_names
            ],
        )

        return self._replies_to_title_dict(result)

    def del_sheets(self, sheet_ids: list[str]):
        data = [{"deleteSheet": {"sheetId": sheet_id}} for sheet_id in sheet_ids]
        self._operation(data)

    def update_sheets(
        self, updates: list[UpdateSheetRequestModel]
    ) -> list[UpdateSheetRequestModel]:
        api = "/open-apis/sheets/v2/spreadsheets/:spreadsheetToken/sheets_batch_update"
        body = {"requests": []}

        for update in updates:
            body["requests"].append(
                {"updateSheet": {"properties": update.model_dump(exclude_none=True)}}
            )

        response = self._request(lark.HttpMethod.POST, api, body=body)
        logger.debug(response.raw)
        resp = json.loads(response.raw.content)  # type: ignore

        results: list[UpdateSheetRequestModel] = []

        for result in resp.get("data").get("replies"):
            properties = result.get("updateSheet").get("properties")
            update = UpdateSheetRequestModel.model_validate(properties)
            results.append(update)

        return results


def build_cell_range(
    start_pointer: tuple[int, int], end_pointer: tuple[int, int]
) -> str:
    """
    build_cell_range 的 Docstring

    :param start_pointer: 说明
    :type start_pointer: tuple[int, int]
    :param end_pointer: 说明
    :type end_pointer: tuple[int, int]
    """
    x = number_to_letters(start_pointer[0])
    y = number_to_letters(end_pointer[0])
    return f"{x}{start_pointer[1]}:{y}{end_pointer[1]}"
