import contextlib
import json
import re
from pathlib import Path

from DrissionPage import ChromiumPage

from xrpa_core.core.logger import logger


def _find_cookie_by_name(page, name: str) -> str | None:
    cookies = page.cookies(all_domains=False)
    filtered = [cookie for cookie in cookies if name in cookie.get("name", "")]
    if len(filtered) == 0:
        logger.warning(f"Cookies 中没有 {name}")
        return None

    first = filtered[0]
    value = first.get("value")
    return value


def get_aid(page: ChromiumPage) -> str:
    """
    获取 aid

    Args:
        page: ChromiumPage 页面对象

    Returns:
        str: aid 值
    """
    script = """
        return (function() {
            if(typeof _accountInfoParam !== 'undefined' && _accountInfoParam){
                return _accountInfoParam.aid
            }
            return null
        })()
    """
    aid = page.run_js(script)

    if not aid:
        aid = _find_cookie_by_name(page, "app_id_unified_seller_env")

    if not aid:
        raise RuntimeError("获取 aid 失败")

    return aid


def get_aid_v2(page: ChromiumPage) -> str:
    """
    通过执行 get_aid.js 获取 aid

    Args:
        page: ChromiumPage 页面对象

    Returns:
        str: aid 值

    Raises:
        RuntimeError: 获取 aid 失败
    """
    js_path = Path(__file__).parent / "js" / "get_aid.js"

    try:
        aid = page.run_js(js_path)
    except Exception as e:
        logger.warning(f"执行 get_aid.js 获取 aid 失败: {e}")
        aid = None

    if not aid:
        aid = _find_cookie_by_name(page, "app_id_unified_seller_env")

    if not aid:
        # 回退到 v1 方式获取
        with contextlib.suppress(RuntimeError):
            aid = get_aid(page)

    if not aid:
        raise RuntimeError("获取 aid 失败")

    return aid


def filter_cookies_by_regex(cookie_list, pattern):
    """
    根据正则表达式过滤 cookies

    Args:
        cookie_list: cookie 列表
        pattern: 正则表达式模式

    Returns:
        list: 过滤后的 cookie 列表
    """
    regex = re.compile(pattern)
    return [
        cookie
        for cookie in cookie_list
        if isinstance(cookie, dict)
        and "name" in cookie
        and regex.search(cookie["name"])
    ]


def get_oec_seller_id_v2(page: ChromiumPage) -> str | None:
    """
    从页面的 localStorage 中读取 ecom_seller_base_menu，
    获取其中的 identifier 值，并返回数字部分

    参数:
        page: DrissionPage 的 ChromiumPage 对象

    返回:
        str: identifier 中的数字部分，如 '7495168096336775923'
        None: 如果未找到或解析失败
    """

    def _get_by_local_storage():
        try:
            # 读取 localStorage 中的 ecom_seller_base_menu
            local_storage_value = page.run_js(
                'return localStorage.getItem("ecom_seller_base_menu");'
            )

            if not local_storage_value:
                # logger.info("未找到 ecom_seller_base_menu")
                return None

            # 解析 JSON
            menu_data = json.loads(local_storage_value)

            # 获取 identifier
            identifier = menu_data.get("identifier")

            if not identifier:
                logger.info("未找到 identifier 字段")
                return None

            # logger.info(f"原始 identifier: {identifier}")

            # 使用正则表达式提取数字部分
            # 匹配格式: ecom_seller_identifier_menu_数字_US
            match = re.search(r"ecom_seller_identifier_menu_(\d+)_", identifier)

            if match:
                number_part = str(match.group(1))
                return number_part
            else:
                logger.info("未能从 identifier 中提取数字")
                return None

        except json.JSONDecodeError as e:
            logger.info(f"JSON 解析错误: {e}")
            return None
        except RuntimeError as e:
            logger.info(f"运行时错误: {e}")
            return None

    oec_selller_id = _get_by_local_storage()

    if not oec_selller_id:
        # logger.info("尝试通过 cookie 获取 oec_seller_id")
        oec_selller_id = _find_cookie_by_name(
            page, "global_seller_id_unified_seller_env"
        )
        if not oec_selller_id:
            oec_selller_id = _find_cookie_by_name(
                page, "oec_seller_id_unified_seller_env"
            )

    return oec_selller_id
