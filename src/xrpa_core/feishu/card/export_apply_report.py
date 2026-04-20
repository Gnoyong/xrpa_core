from pydantic import BaseModel

from xrpa_core.feishu.feishu_notify import build_template_card

CID = "AAq272yxBQZga"
V = "1.0.0"


class ExportApplyReportCard(BaseModel):
    date_str: str
    markdown: str

    def build(self):
        return build_template_card(
            CID,
            V,
            self.model_dump(),
        )
