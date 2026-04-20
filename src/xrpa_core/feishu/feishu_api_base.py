import json
import time
from collections.abc import Callable
from typing import Any, TypeVar
from urllib.parse import urlencode

import lark_oapi as lark
from lark_oapi.api.sheets.v3 import BaseResponse
from requests.exceptions import ConnectionError

from xrpa_core.core.logger import logger

T = TypeVar("T")
R = TypeVar("R", bound=BaseResponse)

MAX_RATE_LIMIT_RETRY_COUNT = 3
DEFAULT_RATE_LIMIT_WAIT_SECONDS = 3


class RateLimitException(RuntimeError):
    def __init__(self, reset_sec: int, *args):
        self.reset_sec = reset_sec
        super().__init__(*args)


class ResponseError(RuntimeError):
    def __init__(self, message: str | None = None, response: Any = None):
        self.response = response
        super().__init__(message)


class NetworkError(RuntimeError):
    pass


def _to_json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_safe(v) for v in value]

    value_attr = getattr(value, "value", None)
    if isinstance(value_attr, (str, int, float, bool)):
        return value_attr

    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        try:
            return _to_json_safe(to_dict())
        except (TypeError, ValueError):
            pass

    return str(value)


def _build_request_context(
    response: BaseResponse, request: lark.BaseRequest | None = None
) -> dict[str, Any]:
    if request is None and response.raw is not None:
        request = getattr(response.raw, "request", None)

    if request is None:
        return {"available": False}

    request_context: dict[str, Any] = {}
    for attr in (
        "http_method",
        "method",
        "uri",
        "paths",
        "queries",
        "query_params",
        "headers",
        "body",
        "token_types",
    ):
        attr_value = getattr(request, attr, None)
        if attr_value is not None:
            request_context[attr] = _to_json_safe(attr_value)

    if not request_context:
        request_context["repr"] = str(request)

    request_context["available"] = True
    return request_context


def _parse_json_response(response: lark.BaseResponse) -> dict[str, Any]:
    """
    返回 dict 格式 response.raw.content 中的 JSON 数据
    """
    if response.raw is None or response.raw.content is None:
        raise ResponseError("响应体为空")
    try:
        data = json.loads(response.raw.content)
    except json.JSONDecodeError as e:
        raise ResponseError(f"响应体不是合法的 JSON 字符串: {e.msg}") from e

    if not isinstance(data, dict):
        raise ResponseError("响应体不是 JSON 对象")
    return data


def _error_handler(response: BaseResponse, request: lark.BaseRequest | None = None):
    content = _parse_json_response(response)
    code = content.get("code")
    request_context = _build_request_context(response, request=request)
    # 处理限流
    if code in (99991400, 90217):
        reset_str = None
        if response.raw is not None and response.raw.headers is not None:
            reset_str = response.raw.headers.get("X-Ogw-Ratelimit-Reset")

        if reset_str is not None:
            raise RateLimitException(reset_sec=int(reset_str))

        if code == 90217:
            raise RateLimitException(reset_sec=DEFAULT_RATE_LIMIT_WAIT_SECONDS)

        detail = {
            "error": "响应头缺失 X-Ogw-Ratelimit-Reset 字段",
            "request": request_context,
            "response": _to_json_safe(content),
        }
        raise ResponseError(json.dumps(detail, indent=4, ensure_ascii=False))

    detail = {
        "code": response.code,
        "msg": response.msg,
        "request": request_context,
        "response": _to_json_safe(content),
    }
    json_content = json.dumps(detail, indent=4, ensure_ascii=False)
    msg = f"failed, detail: \n{json_content}"
    raise ResponseError(msg, response=detail)


def _ensure_response_success(response: Any, request: lark.BaseRequest | None = None):
    success_method = getattr(response, "success", None)
    if success_method is None or not callable(success_method):
        raise ResponseError("响应对象缺少 success 校验方法")

    if not success_method():
        _error_handler(response, request=request)


def _get_rate_limit_wait_seconds(reset_sec: int) -> int:
    now = int(time.time())
    if reset_sec > now:
        wait_seconds = reset_sec - now
    else:
        wait_seconds = reset_sec
    return max(wait_seconds, 1)


def _retry_on_rate_limit(
    action_name: str,
    request_func: Callable[[], T],
    max_retry_count: int = MAX_RATE_LIMIT_RETRY_COUNT,
) -> T:
    for retry_count in range(max_retry_count + 1):
        try:
            return request_func()
        except RateLimitException as err:
            if retry_count >= max_retry_count:
                logger.error(f"{action_name} 限流重试达到上限，停止重试")
                raise

            wait_seconds = _get_rate_limit_wait_seconds(err.reset_sec)
            logger.warning(
                f"{action_name} 触发限流，{wait_seconds} 秒后进行第 {retry_count + 1} 次重试"
            )
            time.sleep(wait_seconds)
        except ConnectionError as e:
            logger.error(f"{action_name} 发生网络错误 {e}，等待重试...")
            time.sleep(3)

    raise ResponseError("重试流程异常结束")


class FeishuApiBase:
    """
    飞书接口基础能力：统一封装请求构建、错误处理与限流重试
    """

    def __init__(self, client, spreadsheet_token: str | None = None):
        self.client = client
        self.spreadsheet_token = spreadsheet_token
        self.default_token_type = lark.AccessTokenType.TENANT

    def _request(
        self,
        method: lark.HttpMethod,
        uri: str,
        body: dict | None = None,
        paths: dict | None = None,
        query_params: dict | None = None,
        token_type: lark.AccessTokenType | None = None,
        action_name: str | None = None,
    ) -> lark.BaseResponse:
        def _do_request() -> lark.BaseResponse:
            request_paths: dict = {}
            if self.spreadsheet_token:
                request_paths["spreadsheetToken"] = self.spreadsheet_token
            if paths is not None:
                request_paths.update(paths)

            request_uri = uri
            if query_params:
                request_uri = f"{uri}?{urlencode(query_params)}"

            request_token_type = token_type or self.default_token_type
            builder = (
                lark.BaseRequest.builder()
                .http_method(method)
                .uri(request_uri)
                .paths(request_paths)
                .token_types({request_token_type})
            )

            if body is not None:
                builder = builder.body(body)

            request: lark.BaseRequest = builder.build()

            response: lark.BaseResponse = self.client.request(request)
            _ensure_response_success(response, request=request)
            return response

        retry_action_name = action_name or f"请求飞书接口 {uri}"
        return _retry_on_rate_limit(retry_action_name, _do_request)

    def _request_json(
        self,
        method: lark.HttpMethod,
        uri: str,
        body: dict | None = None,
        paths: dict | None = None,
        query_params: dict | None = None,
        token_type: lark.AccessTokenType | None = None,
        action_name: str | None = None,
    ) -> dict:
        response = self._request(
            method,
            uri,
            body=body,
            paths=paths,
            query_params=query_params,
            token_type=token_type,
            action_name=action_name,
        )
        return _parse_json_response(response)

    def _invoke_response(self, action_name: str, request_func: Callable[[], R]) -> R:
        def _do_request() -> R:

            response = request_func()
            _ensure_response_success(response)
            return response

        return _retry_on_rate_limit(action_name, _do_request)
