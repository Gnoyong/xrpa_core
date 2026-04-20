import json
from typing import Any

from lark_oapi.api.cardkit.v1 import (
    ContentCardElementRequest,
    ContentCardElementRequestBody,
    ContentCardElementResponse,
    CreateCardElementRequest,
    CreateCardElementRequestBody,
    CreateCardElementResponse,
    CreateCardRequest,
    CreateCardRequestBody,
    CreateCardResponse,
    DeleteCardElementRequest,
    DeleteCardElementRequestBody,
    DeleteCardElementResponse,
    PatchCardElementRequest,
    PatchCardElementRequestBody,
    PatchCardElementResponse,
    UpdateCardElementRequest,
    UpdateCardElementRequestBody,
    UpdateCardElementResponse,
)

from xrpa_core.feishu.feishu_api_base import FeishuApiBase, ResponseError


class CardApi(FeishuApiBase):
    """
    飞书卡片相关接口封装
    """

    def _create_card_entity(
        self,
        card_type: str,
        data: str | dict,
    ) -> str:
        """
        创建卡片实体，返回卡片ID

        Args:
            card_type: 卡片类型，可选值：
                - card_json：由卡片 JSON 代码构建的卡片
                - template：由卡片搭建工具搭建的卡片模板
            data: 卡片数据。如果传入 dict，会自动转为 JSON 字符串

        Returns:
            卡片ID
        """
        data_str = json.dumps(data) if isinstance(data, dict) else data

        def _do_request() -> CreateCardResponse:
            request: CreateCardRequest = (
                CreateCardRequest.builder()
                .request_body(
                    CreateCardRequestBody.builder()
                    .type(card_type)
                    .data(data_str)
                    .build()
                )
                .build()
            )
            return self.client.cardkit.v1.card.create(request)

        response = self._invoke_response("创建卡片实体", _do_request)

        if not response.data or not response.data.card_id:
            raise ResponseError("创建卡片实体失败，未返回 card_id")

        return response.data.card_id

    def create_template_card(
        self,
        template_id: str,
        template_version_name: str = "1.0.0",
        template_variable: dict[str, Any] | None = None,
    ) -> str:
        """
        创建模板卡片实体（便捷方法）

        Args:
            template_id: 卡片模板ID
            template_version_name: 模板版本名称
            template_variable: 模板变量

        Returns:
            卡片ID
        """
        data = {
            "template_id": template_id,
            "template_version_name": template_version_name,
            "template_variable": {},
        }
        if template_variable:
            data["template_variable"] = template_variable

        return self._create_card_entity(card_type="template", data=data)

    def create_json_card(self, card_json: str | dict) -> str:
        """
        创建 JSON 卡片实体（便捷方法）

        Args:
            card_json: 卡片 JSON 代码（支持 dict 或 JSON 字符串）

        Returns:
            卡片ID
        """
        return self._create_card_entity(card_type="card_json", data=card_json)

    def update_card_element_content(
        self, card_id: str, element_id: str, content: str, sequence: int
    ) -> ContentCardElementResponse:
        """
        更新卡片元素内容

        Args:
            card_id: 卡片ID
            element_id: 元素ID
            content: 元素内容
            sequence: 序列号（用于保证更新顺序）
        """

        def _do_request() -> ContentCardElementResponse:
            request: ContentCardElementRequest = (
                ContentCardElementRequest.builder()
                .element_id(element_id)
                .card_id(card_id)
                .request_body(
                    ContentCardElementRequestBody.builder()
                    .content(content)
                    .sequence(sequence)
                    .build()
                )
                .build()
            )
            return self.client.cardkit.v1.card_element.content(request)

        response = self._invoke_response("更新卡片元素", _do_request)
        return response

    def add_card_element(
        self,
        card_id: str,
        elements: str | list[dict],
        sequence: int,
        append_type: str = "append",
        target_element_id: str | None = None,
        uuid: str | None = None,
    ) -> CreateCardElementResponse:
        """
        为指定卡片实体新增组件

        Args:
            card_id: 卡片实体 ID
            elements: 添加的组件列表（支持 JSON 字符串或列表）
            sequence: 操作卡片的序号，需严格递增
            type: 添加组件的方式，可选值：
                - insert_before：在目标组件前插入
                - insert_after：在目标组件后插入
                - append：在卡片或容器组件末尾添加（默认）
            target_element_id: 目标组件的 ID
                - 当 type 为 insert_before、insert_after 时必填
                - 当 type 为 append 时，仅支持容器类组件
            uuid: 幂等 ID，用于保证相同批次的操作只进行一次

        Returns:
            CreateCardElementResponse
        """
        elements_str = json.dumps(elements) if isinstance(elements, list) else elements

        def _do_request() -> CreateCardElementResponse:
            builder = (
                CreateCardElementRequestBody.builder()
                .type(append_type)
                .sequence(sequence)
                .elements(elements_str)
            )

            if target_element_id:
                builder = builder.target_element_id(target_element_id)
            if uuid:
                builder = builder.uuid(uuid)

            request: CreateCardElementRequest = (
                CreateCardElementRequest.builder()
                .card_id(card_id)
                .request_body(builder.build())
                .build()
            )
            return self.client.cardkit.v1.card_element.create(request)

        response = self._invoke_response("新增卡片组件", _do_request)
        return response

    def update_card_element(
        self,
        card_id: str,
        element_id: str,
        element: str | dict,
        sequence: int,
        uuid: str | None = None,
    ) -> UpdateCardElementResponse:
        """
        更新卡片实体中的指定组件为新组件

        Args:
            card_id: 卡片实体 ID
            element_id: 要更新的组件 ID
            element: 新组件的完整 JSON 数据（支持 JSON 字符串或 dict）
            sequence: 操作卡片的序号，需严格递增
            uuid: 幂等 ID，用于保证相同批次的操作只进行一次

        Returns:
            UpdateCardElementResponse
        """
        element_str = json.dumps(element) if isinstance(element, dict) else element

        def _do_request() -> UpdateCardElementResponse:
            builder = (
                UpdateCardElementRequestBody.builder()
                .element(element_str)
                .sequence(sequence)
            )

            if uuid:
                builder = builder.uuid(uuid)

            request: UpdateCardElementRequest = (
                UpdateCardElementRequest.builder()
                .card_id(card_id)
                .element_id(element_id)
                .request_body(builder.build())
                .build()
            )
            return self.client.cardkit.v1.card_element.update(request)

        response = self._invoke_response("更新卡片组件", _do_request)
        return response

    def patch_card_element(
        self,
        card_id: str,
        element_id: str,
        partial_element: str | dict,
        sequence: int,
        uuid: str | None = None,
    ) -> PatchCardElementResponse:
        """
        更新卡片实体中指定组件的属性（不支持修改 tag 属性）

        Args:
            card_id: 卡片实体 ID
            element_id: 要更新的组件 ID
            partial_element: 组件的新配置项字段（支持 JSON 字符串或 dict）
            sequence: 操作卡片的序号，需严格递增
            uuid: 幂等 ID，用于保证相同批次的操作只进行一次

        Returns:
            PatchCardElementResponse
        """
        partial_element_str = (
            json.dumps(partial_element)
            if isinstance(partial_element, dict)
            else partial_element
        )

        def _do_request() -> PatchCardElementResponse:
            builder = (
                PatchCardElementRequestBody.builder()
                .partial_element(partial_element_str)
                .sequence(sequence)
            )

            if uuid:
                builder = builder.uuid(uuid)

            request: PatchCardElementRequest = (
                PatchCardElementRequest.builder()
                .card_id(card_id)
                .element_id(element_id)
                .request_body(builder.build())
                .build()
            )
            return self.client.cardkit.v1.card_element.patch(request)

        response = self._invoke_response("更新卡片组件属性", _do_request)
        return response

    def delete_card_element(
        self,
        card_id: str,
        element_id: str,
        sequence: int,
        uuid: str | None = None,
    ) -> DeleteCardElementResponse:
        """
        删除指定卡片实体中的组件

        注意：删除容器类组件时，容器中内嵌的组件将一并被删除。

        Args:
            card_id: 卡片实体 ID
            element_id: 要删除的组件 ID
            sequence: 操作卡片的序号，需严格递增
            uuid: 幂等 ID，用于保证相同批次的操作只进行一次

        Returns:
            DeleteCardElementResponse
        """

        def _do_request() -> DeleteCardElementResponse:
            builder = DeleteCardElementRequestBody.builder().sequence(sequence)

            if uuid:
                builder = builder.uuid(uuid)

            request: DeleteCardElementRequest = (
                DeleteCardElementRequest.builder()
                .card_id(card_id)
                .element_id(element_id)
                .request_body(builder.build())
                .build()
            )
            return self.client.cardkit.v1.card_element.delete(request)

        response = self._invoke_response("删除卡片组件", _do_request)
        return response
