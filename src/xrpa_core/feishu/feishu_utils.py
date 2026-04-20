import json

from lark_oapi.api.wiki.v2 import GetNodeSpaceRequest, GetNodeSpaceResponse

from xrpa_core.core.logger import logger
from xrpa_core.feishu.feishu_client import get_client


def get_node_space(token: str):
    """
    get_node_space 的 Docstring

    :param token: 说明
    :type token: str
    """
    client = get_client()
    # 构造请求对象
    request: GetNodeSpaceRequest = (
        GetNodeSpaceRequest.builder().obj_type("wiki").token(token).build()
    )

    # 发起请求
    response: GetNodeSpaceResponse = client.wiki.v2.space.get_node(request)

    # 处理失败返回
    if not response.success():
        logger.error(
            f"client.wiki.v2.space.get_node failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}, resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
        )
        return

    # 处理业务结果
    # logger.info(lark.JSON.marshal(response.data, indent=4))
