from xrpa_core.feishu.feishu_client import get_client

from .feishu_card import CardApi

card_api = CardApi(get_client())

__all__ = ["card_api"]
