class CardTemplate:
    def __init__(
        self, template_id: str, template_version: str, variable: dict | None = None
    ):
        self.template_id = template_id
        self.template_version = template_version
        self.variable = variable

    def build_template_card(
        self,
    ):
        """
        构建模板卡片
        """
        return {
            "type": "template",
            "data": {
                "template_id": self.template_id,
                "template_version_name": self.template_version,
                "template_variable": self.variable or {},
            },
        }
