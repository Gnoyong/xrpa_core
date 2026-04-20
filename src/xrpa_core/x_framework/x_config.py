import pathlib
import re
from pathlib import Path

import yaml
from jsonpath_ng.parser import parse

from xrpa_core.core.logger import logger
from xrpa_core.x_framework.x_db import ConfigService, DataSource
from xrpa_core.x_framework.x_utils import track

MODULE_DIR = pathlib.Path(__file__).parent.parent.parent

# 根目录（假设 config.yml 在模块根目录）
DEFAULT_CONFIG_FILE = MODULE_DIR.parent / "config.yml"

_config_cache = None
_config_path = None


def set_config_path(path: str | Path | None) -> None:
    global _config_path, _config_cache
    if not path:
        return
    _config_path = Path(path).expanduser()
    _config_cache = None


def load_config(path: str | Path | None = None) -> dict:
    """
    从模块目录读取配置文件
    """
    config_path = Path(path or _config_path or DEFAULT_CONFIG_FILE).resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # logger.info(f"加载配置文件: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _ensure_config_loaded() -> dict:
    global _config_cache
    if _config_cache is None:
        _config_cache = load_config()
    return _config_cache


def get_value_by_jsonpath(data: dict, path: str):
    """
    根据 JSONPath 路径从 dict 中获取值
    :param data: dict 对象
    :param path: JSONPath 字符串，例如 '$.store.book[0].title'
    :return: 匹配到的值列表（可能有多个）
    """
    if not data or data == {}:
        raise RuntimeError("data 为空")
    try:
        jsonpath_expr = parse(path)
        matches = [match.value for match in jsonpath_expr.find(data)]
        return matches[0] if matches else None
    except Exception as e:
        logger.info(f"解析 JSONPath 出错: {e}")
        return None


def get_values_by_jsonpath(data: dict, path: str):
    """
    根据 JSONPath 路径从 dict 中获取值
    :param data: dict 对象
    :param path: JSONPath 字符串，例如 '$.store.book[0].title'
    :return: 匹配到的值列表（可能有多个）
    """
    try:
        jsonpath_expr = parse(path)
        matches = [match.value for match in jsonpath_expr.find(data)]
        return matches
    except Exception as e:
        logger.info(f"解析 JSONPath 出错: {e}")
        return None


def get_global_config(key: str):
    ds = DataSource()
    service = ConfigService(ds)
    kv = service.read(key)
    return kv.value


def resolve_path(path: str):
    match = re.search(r"(?<=\{)(.*?)(?=\})", path)
    if not match:
        return path
    drive_name = match.group(0)
    drive = get_network_drive_by_name(drive_name)
    if not drive:
        raise Exception(f"未能找到路径 {path!r} 中 {drive_name!r} 的盘符")
    return path.replace(f"{{{drive_name}}}", drive)


def get_xconfig_path(jsonpath: str):
    return resolve_path(get_xconfig_item(jsonpath))


def get_xconfig_item(jsonpath: str):
    # if not jsonpath.startswith("$."):
    # jsonpath = f"$.{jsonpath}"
    item = get_value_by_jsonpath(_ensure_config_loaded(), jsonpath)
    if item == None:
        raise Exception(track(f"没有在配置文件中找到 {jsonpath!r} 项"))
    else:
        return item


def get_xconfig_item_v2(jsonpath: str):
    cfg = _ensure_config_loaded()
    x_mode = get_value_by_jsonpath(cfg, "mode")
    if x_mode == "dev":
        dev_jsonpath = "dev." + jsonpath
        if get_value_by_jsonpath(cfg, dev_jsonpath) != None:
            jsonpath = dev_jsonpath

    item = get_value_by_jsonpath(cfg, jsonpath)
    if item == None:
        raise Exception(track(f"没有在配置文件中找到 {jsonpath!r} 项"))
    else:
        return item
