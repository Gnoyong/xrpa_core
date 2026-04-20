from xrpa_core.command.apply.exporter.command import ExportApplicationsCommand
from xrpa_core.config.config import get_config

config = get_config()
id_to_store = config.get_stores()
ExportApplicationsCommand()._send_result(id_to_store.values())
