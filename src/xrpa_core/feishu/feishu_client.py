import lark_oapi as lark
from lark_oapi import Client

from xrpa_core.x_framework.x_config import get_xconfig_item_v2


class FeishuApp:
    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret


def get_client(app_id: str = None, app_secret: str = None) -> Client:
    app_id = get_xconfig_item_v2("feishu_app.fs_rpa.app_id")
    app_secret = get_xconfig_item_v2("feishu_app.fs_rpa.app_secret")
    return (
        lark.Client.builder()
        .app_id(app_id)
        .app_secret(app_secret)
        .log_level(lark.LogLevel.INFO)
        .build()
    )
