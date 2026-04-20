"""
安全执行 JavaScript 的工具函数
"""

from typing import Any

from DrissionPage._pages.chromium_base import ChromiumBase
from DrissionPage.errors import ContextLostError

from xrpa_core.core.logger import logger


def run_js_safe(
    page: ChromiumBase,
    script: str,
    *args: Any,
    timeout: float = 30,
) -> Any:
    """
    安全执行 JavaScript，捕获 ContextLostError 异常并重试。

    Args:
        page: ChromiumPage 或 ChromiumTab 实例
        script: JavaScript 代码
        *args: 传递给 run_js 的参数
        timeout: 等待页面加载完成的超时时间（秒）

    Returns:
        JavaScript 执行结果

    Raises:
        ContextLostError: 重试后仍然失败
        其他异常: 直接抛出
    """
    try:
        return page.run_js(script, *args)
    except ContextLostError:
        logger.warning("检测到 ContextLostError，等待页面重新加载后重试...")
        page.wait.doc_loaded(timeout=timeout)
        return page.run_js(script, *args)
