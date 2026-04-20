import base64
import hashlib
import hmac
import time
import traceback

import requests

from xrpa_core.x_framework.x_config import get_xconfig_item_v2

from .card import ExceptionCard


def gen_sign(timestamp, secret):
    # 拼接timestamp和secret
    string_to_sign = f"{timestamp}\n{secret}"
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()

    # 对结果进行base64处理
    sign = base64.b64encode(hmac_code).decode("utf-8")

    return sign


def _with_sign(body: dict, secret: str | None) -> dict:
    if not secret:
        return body

    timestamp = str(int(time.time()))
    sign = gen_sign(timestamp, secret)

    signed = {"timestamp": timestamp, "sign": sign}
    signed.update(body)
    return signed


def build_body(text: str, secret: str | None):
    """
    构建消息 body
    """
    body = {"msg_type": "text", "content": {"text": text}}
    return _with_sign(body, secret)


def build_card_body(card: dict, secret: str | None):
    """
    构建卡片消息 body
    """
    body = {"msg_type": "interactive", "card": card}
    return _with_sign(body, secret)


def build_template_card(
    template_id: str,
    template_version_name: str,
    template_variable: dict | None = None,
):
    """
    构建模板卡片
    """
    return {
        "type": "template",
        "data": {
            "template_id": template_id,
            "template_version_name": template_version_name,
            "template_variable": template_variable or {},
        },
    }


def feishu_notify_by_key(text: str, webhook_key: str = "webhook"):
    webhook_conf = get_xconfig_item_v2(webhook_key)
    secret = webhook_conf.get("secret") if webhook_conf else None
    url = webhook_conf.get("url") if webhook_conf else None

    if not url:
        return

    body = build_body(text, secret)
    requests.post(url, json=body, timeout=6)


def feishu_notify(text):
    feishu_notify_by_key(text, "webhook")


def feishu_notify_card(card: dict, webhook_key: str = "dev_webhook"):
    webhook_conf = get_xconfig_item_v2(webhook_key)
    secret = webhook_conf.get("secret") if webhook_conf else None
    url = webhook_conf.get("url") if webhook_conf else None

    if not url:
        return

    body = build_card_body(card, secret)
    requests.post(url, json=body, timeout=6)


def feishu_exception_notify(app_info: str, exc: Exception):
    webhook_conf = get_xconfig_item_v2("dev_webhook")
    secret = webhook_conf.get("secret")
    url = webhook_conf.get("url")

    stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    exc_msg = f"{type(exc).__name__}: {str(exc)}"

    card = ExceptionCard(app_info, exc_msg, stack).build_template_card()
    body = build_card_body(card, secret)

    requests.post(url, json=body, timeout=6)
