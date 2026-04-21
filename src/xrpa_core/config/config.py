import tempfile

from xrpa_core.entity.zb_credential import ZbCredential


class Config:
    db_url = "postgresql+psycopg2://postgres:123456@localhost:5432/tkauto"

    zb_credential = {
        "tk_fully": ZbCredential("尾号3107的公司26", "邱锦辉", "QJH@2563"),
        "tk_creator": ZbCredential("尾号4270的公司16", "刘智勇", "j{UDmK=A"),
    }

    def __init__(self):
        self.cache_dir = tempfile.gettempdir()

def get_config():
    return Config()
