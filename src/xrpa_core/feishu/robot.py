from typing import Literal

import lark_oapi as lark

from xrpa_core.feishu.feishu_api_base import FeishuApiBase

MemberIdType = Literal["open_id", "union_id", "user_id"]


class FeishuRobot(FeishuApiBase):
    """
    Robot SDK: 封装 IM 相关接口
    """

    def __init__(
        self, client, token_type: lark.AccessTokenType = lark.AccessTokenType.TENANT
    ):
        super().__init__(client=client)
        self.default_token_type = token_type

    def get_chat_members_page(
        self,
        chat_id: str,
        member_id_type: MemberIdType = "open_id",
        page_size: int = 20,
        page_token: str | None = None,
    ) -> dict:
        """
        获取群成员列表单页数据。
        """
        if not chat_id:
            raise ValueError("chat_id 不能为空")

        if page_size <= 0 or page_size > 100:
            raise ValueError("page_size 必须在 1 到 100 之间")

        query_params: dict = {
            "member_id_type": member_id_type,
            "page_size": page_size,
        }
        if page_token:
            query_params["page_token"] = page_token

        return self._request_json(
            lark.HttpMethod.GET,
            "/open-apis/im/v1/chats/:chat_id/members",
            paths={"chat_id": chat_id},
            query_params=query_params,
            action_name=f"获取群成员列表 {chat_id}",
        )

    def get_chat_members(
        self,
        chat_id: str,
        member_id_type: MemberIdType = "open_id",
        page_size: int = 100,
    ) -> list[dict[str, str | None]]:
        """
        自动遍历分页，返回成员的名字与 ID。
        """
        members: list[dict[str, str | None]] = []
        next_page_token: str | None = None

        while True:
            response = self.get_chat_members_page(
                chat_id=chat_id,
                member_id_type=member_id_type,
                page_size=page_size,
                page_token=next_page_token,
            )

            data = response.get("data") or {}
            items = data.get("items") or []

            for item in items:
                if not isinstance(item, dict):
                    continue

                members.append(
                    {
                        "name": item.get("name"),
                        "member_id": item.get("member_id"),
                    }
                )

            if not data.get("has_more"):
                break

            next_page_token = data.get("page_token")
            if not next_page_token:
                break

        return members
