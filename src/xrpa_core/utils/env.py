from xrpa_core.service.vmenv_service import VmEnvService

from ..config import app_config


def get_creator_env():
    return VmEnvService("tk-creator", app_config.zb_credential["tk_creator"])


def get_fully_env():
    return VmEnvService("tk-fully", app_config.zb_credential["tk_fully"])
