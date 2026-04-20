from pydantic import BaseModel

from xrpa_core.feishu.feishu_notify import build_template_card

TID = "AAq27dSwJZSpq"
V = "1.0.5"


class _Item(BaseModel):
    store: str
    verified: int
    unverified: int
    total: int


class VerifyStatisticsCard(BaseModel):
    total: int
    verified: int
    unverified: int
    table: list[_Item]
    date_str: str


def build(vs_card: VerifyStatisticsCard):
    return build_template_card(
        TID,
        V,
        vs_card.model_dump(),
    )
