"""
TikTok API 基础能力
"""

import base64
import json
import os
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

from DrissionPage import ChromiumPage
from DrissionPage._units.listener import Listener
from DrissionPage.errors import ContextLostError
from xrpa_core.model.store import Shop

from xrpa_core.core.logger import logger
from xrpa_core.lib.js_runner import run_js_safe
from xrpa_core.utils.page import get_aid_v2, get_oec_seller_id_v2


class PageUnloadException(Exception):
    pass


class ExportPendingError(RuntimeError):
    """文件还在导出中"""


class ResponseError(RuntimeError):
    """接口响应异常"""

    def __init__(self, message: str, response: dict | None = None):
        self.response = response
        super().__init__(message)


class BusinessError(ResponseError):
    """接口返回了业务错误，通常伴随 code != 0 的 JSON 响应"""


class HttpStatusError(ResponseError):
    """接口返回了异常 HTTP 状态码"""


class UnauthorizedError(BusinessError):
    def __init__(self, store_id: str):
        self.store_id = store_id
        super().__init__(f"店铺 {store_id} 授权过期")


class NetworkError(RuntimeError):
    pass


class TikTokBaseAPI:
    """TikTok API 基础类，封装通用 fetch / URL / 下载逻辑。"""

    class FetchConfig:
        def __init__(
            self,
            url: str,
            method: str = "GET",
            headers: dict[str, str] | None = None,
            body: dict | str | None = None,
            credentials: str = "include",
        ):
            self.url = url
            self.method = method
            self.headers = headers or {"content-type": "application/json"}
            self.body = body
            self.credentials = credentials

    def _find_page_by_base_url(
        self, page: ChromiumPage, base_url: str
    ) -> ChromiumPage | None:
        """在当前浏览器已有标签页中查找 URL 包含 base_url 的页面。"""
        current_url = page.url or ""
        if base_url in current_url:
            return page

        for tab in page.browser.get_tabs():
            tab_url = tab.url or ""

            if base_url in tab_url:
                logger.info(f"找到符合预期 URL 的标签页，准备使用: {tab_url}")
                return cast(ChromiumPage, tab)

        return None

    def __init__(
        self,
        store: Shop,
        page: ChromiumPage,
        base_url: str,
        initial_path: str = "",
        timezone_name: str = "Asia/Hong_Kong",
        shop_region: str = "US",
        auth_api: str | None = None,
    ):
        self.store = store
        initial_path = initial_path.lstrip("/")

        self.base_url = base_url.rstrip("/")
        self.page: ChromiumPage = page.new_tab("about:blank")

        self.auth_api = auth_api

        if auth_api is not None:
            self.page.listen.start(auth_api)

        self.page.get(f"{self.base_url}/{initial_path}")
        self.shop_region = shop_region
        self.timezone_name = timezone_name
        self.js_script = Path(r"src\tkauto\crawler\js\tiktok_fetch.js").read_text(
            encoding="utf-8"
        )

        self.page.wait.doc_loaded()

        def fetch_aid(self):
            try:
                return get_aid_v2(self.page)
            except ContextLostError:
                logger.info("页面上下文丢失，等待重新加载后重试...")
                self.page.wait.doc_loaded()
                return get_aid_v2(self.page)

        self.aid = fetch_aid(self)

        if not self.aid:
            raise RuntimeError("无法从页面获取 aid，无法继续执行 API 请求")

        oec_seller_id = get_oec_seller_id_v2(self.page)

        if not oec_seller_id:
            raise RuntimeError("无法从页面获取 oec_seller_id，无法继续执行 API 请求")

        self.oec_seller_id = oec_seller_id

        if auth_api is not None and not self._check_auth_valid(self.page.listen):
            raise UnauthorizedError(self.store.id)

        # if auth_api is not None and self.page.listen:
        #     try:
        #         self.page.listen.stop()
        #     except AttributeError:
        #         logger.warning("停止监听时发生 AttributeError")

    def _check_auth_valid(self, listener: Listener) -> bool:
        """
        检查授权是否有效，子类可重写此方法实现自定义验证逻辑。

        Returns:
            bool: 授权是否有效
        """
        _ = listener
        return True

    def __del__(self):
        self.close()

    def close(self):
        """关闭当前页面。"""
        try:
            self.page.close()
        except Exception:
            logger.warning("关闭页面时发生异常，可能页面已被关闭")

    def _build_params(self) -> dict[str, str]:
        return {
            "user_language": "en",
            "aid": self.aid,
            "app_name": "i18n_ecom_alliance",
            "device_id": "0",
            "device_platform": "web",
            "cookie_enabled": "true",
            "screen_width": "1920",
            "screen_height": "1080",
            "browser_language": "zh-HK",
            "browser_platform": "Win32",
            "browser_name": "Mozilla",
            "browser_version": "5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
            "browser_online": "true",
            "timezone_name": self.timezone_name,
            "oec_seller_id": self.oec_seller_id,
            "shop_region": self.shop_region,
        }

    def _execute_fetch_v2(self, fetch_config: FetchConfig) -> dict:
        js_fetch_config = {
            "url": fetch_config.url,
            "method": fetch_config.method,
            "headers": fetch_config.headers,
            "body": fetch_config.body,
            "credentials": fetch_config.credentials,
        }
        for attempt in range(3):
            try:
                return self._execute_fetch(js_fetch_config)
            except (RuntimeError, NetworkError) as exc:
                if attempt >= 3 - 1:
                    raise
                logger.warning(f"请求失败，准备重试 ({attempt + 1}/3): {exc}")
                time.sleep(3)

        return self._execute_fetch(js_fetch_config)

    def _save_error_to_file(
        self,
        fetch_config: dict,
        reason: str,
        json_result: dict[str, Any] | None = None,
        response: Any | None = None,
        extra: str | None = None,
    ):
        """记录 fetch 错误上下文日志。"""
        payload = {
            "reason": reason,
            "request": fetch_config,
            "status": (
                json_result.get("status") if isinstance(json_result, dict) else None
            ),
            "status_text": (
                json_result.get("statusText") if isinstance(json_result, dict) else None
            ),
            "headers": (
                json_result.get("headers") if isinstance(json_result, dict) else None
            ),
            "body": response,
            "js_result": json_result,
            "extra": extra,
        }
        # 保存错误详情到临时文件
        temp_dir = Path(tempfile.gettempdir()) / "tkauto_errors"
        temp_dir.mkdir(parents=True, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        error_file = temp_dir / f"fetch_error_{timestamp}.json"
        error_file.write_text(
            json.dumps(payload, ensure_ascii=False, default=str, indent=2),
            encoding="utf-8",
        )
        return str(error_file)

    def _execute_fetch(self, fetch_config: dict) -> dict:
        """
        在页面上下文中执行 fetch，并处理常见的错误和重试逻辑。
        """

        def _save_error(
            reason: str,
            json_result: dict[str, Any] | None = None,
            response: Any | None = None,
            extra: str | None = None,
        ):
            file_path = self._save_error_to_file(
                fetch_config, reason, json_result, response, extra
            )
            logger.warning(f"错误详情已保存到: {file_path}")

        def wait_for_fetch_modified(
            page: ChromiumPage,
            default_fetch_str: str,
            timeout: float = 120,
            interval: float = 0.5,
        ):
            default = default_fetch_str.replace(" ", "")
            start = time.time()

            while True:
                window_fetch = run_js_safe(
                    page,
                    """
                return String(window.fetch)
            """,
                ).replace(" ", "")

                if window_fetch != default:
                    return True

                if time.time() - start >= timeout:
                    raise TimeoutError("window.fetch 在 120 秒内仍未被修改")

                time.sleep(interval)

        wait_for_fetch_modified(self.page, "fetch() { [native code] }")

        js_result = run_js_safe(self.page, self.js_script, fetch_config)

        if isinstance(js_result, str):
            try:
                json_result = json.loads(js_result)
            except json.JSONDecodeError as exc:
                _save_error(
                    "JS 返回非 JSON",
                    extra=f"raw_js_result={js_result}",
                )
                raise RuntimeError(f"JavaScript 返回非 JSON: {js_result}") from exc
        else:
            json_result = js_result

        if not isinstance(json_result, dict):
            _save_error(
                "JS 返回结构异常",
                extra=f"result_type={type(json_result)}",
            )
            raise RuntimeError(f"JavaScript 返回结构异常: {json_result}")

        ok = json_result.get("ok", None)

        if ok is None:
            _save_error("JavaScript 执行失败", json_result=json_result)
            raise RuntimeError("JavaScript 执行失败")

        if not ok:
            error = json_result.get("error", "未知错误")
            step = json_result.get("step", "未知阶段")
            error_code = json_result.get("code")
            error_name = json_result.get("name")
            status = json_result.get("status")
            response = json_result.get("response")
            error_extra = (
                f"error_code={error_code}; error={error}; " f"error_name={error_name}"
            )
            _save_error(
                f"JS fetch 失败 [{step}]",
                json_result=json_result,
                response=response,
                extra=error_extra,
            )

            if error_code == "FETCH_TIMEOUT":
                raise TimeoutError("请求超时，可能是网络问题或服务器响应过慢")

            if (
                error_code == "FETCH_NETWORK_ERROR"
                or error_code == "FETCH_RESPONSE_INVALID"
            ):
                raise NetworkError("接口返回了无效响应，可能是网络异常或服务器错误")

            raise RuntimeError(f"请求失败 [{step}]: {error}")

        response = json_result.get("response")
        status = json_result.get("status")
        content_type = str(json_result.get("contentType") or "").lower()
        response_is_json = bool(json_result.get("responseIsJson"))

        if isinstance(status, int) and status >= 400:
            _save_error(
                "HTTP 状态码异常",
                json_result=json_result,
                response=response,
            )
            raise HttpStatusError(
                f"接口返回了异常 HTTP 状态码: {status}", response=response
            )

        if isinstance(response, str) and (
            response_is_json or "application/json" in content_type
        ):
            try:
                response = json.loads(response)
            except json.JSONDecodeError as exc:
                _save_error(
                    "接口 JSON 解析失败",
                    json_result=json_result,
                    response=response,
                    extra=f"json_error={exc}",
                )
                raise

        if not isinstance(response, dict):
            _save_error(
                "接口返回了非 JSON 响应",
                json_result=json_result,
                response=response,
            )
            raise ResponseError("接口返回了非 JSON 响应")

        code = response.get("code")

        if isinstance(response, dict) and code is not None and code != 0:
            _save_error(
                "接口业务状态异常",
                json_result=json_result,
                response=response,
            )

            if code == 10000:
                raise PageUnloadException("登录状态异常，页面可能未登录或未正确加载")
            elif code == 11000:
                raise UnauthorizedError(self.store.id)
            elif code == 98001001:
                raise NetworkError("接口返回了系统错误")

            raise BusinessError(f"接口响应异常: {response}", response=response)

        return response

    def _build_url(
        self,
        api: str,
        additional_param: dict | None = None,
        only_additional: bool = False,
    ) -> str:
        base_params = self._build_params()
        extra_params = additional_param or {}

        params = {
            **base_params,
            **extra_params,
        }

        if only_additional:
            params = extra_params

        query = urlencode(params, doseq=True)
        return f"{self.base_url.rstrip('/')}/{api.lstrip('/')}?{query}"

    def download(self, url: str, save_dir: str | None) -> str:
        result = run_js_safe(
            self.page,
            f"""
            return fetch("{url}", {{
                headers: {{
                    "accept": "*text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "same-origin",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1"
                }},
                credentials: "include",
            }}).then(async r => {{
                const contentType = r.headers.get("content-type") || "";

                if (contentType.includes("application/json")) {{
                    const json = await r.json();
                    return {{
                        is_json: true,
                        json
                    }};
                }}

                const disposition = r.headers.get("content-disposition") || "";
                let filename = "download.xlsx";

                let match = disposition.match(/filename="?([^"]+)"?/);
                if (match) {{
                    filename = match[1];
                }}

                const buf = await r.arrayBuffer();
                let bytes = new Uint8Array(buf);
                let binary = "";
                for (let b of bytes) {{
                    binary += String.fromCharCode(b);
                }}

                return {{
                    is_json: false,
                    filename,
                    b64: btoa(binary)
                }};
            }});
            """,
        )

        if result.get("is_json"):
            json_data = result["json"]
            if not isinstance(json_data, dict):
                raise RuntimeError(f"接口返回了 JSON，但格式不正确: {json_data}")

            if json_data.get("code") == 28001001:
                raise ExportPendingError(f"Export still in progress: {json_data}")
            raise RuntimeError(f"Export failed, got JSON: {json_data}")

        filename = result["filename"]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        unique_suffix = uuid.uuid4().hex[:8]
        filename = f"{timestamp}_{unique_suffix}_{filename}"
        if not save_dir:
            save_dir = os.fspath(Path.home() / "Downloads")
        file_path = os.path.join(save_dir, filename)

        if Path(file_path).exists():
            raise RuntimeError(f"文件已存在: {file_path}")

        with open(file_path, "wb") as f:
            f.write(base64.b64decode(result["b64"]))

        return file_path
