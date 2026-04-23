import time
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar("T")


def retry[T](
    func: Callable[..., T],
    *args,
    retries: int = 3,
    delay: float = 1,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    retry_if: Callable[[Exception], bool] | None = None,
    on_retry: Callable[[int, Exception], Any] | None = None,
    **kwargs,
) -> T:
    """
    通用重试函数

    参数:
        func: 要执行的函数
        retries: 最大重试次数
        delay: 每次重试间隔秒数
        exceptions: 需要捕获并重试的异常类型
        retry_if: 自定义判断是否需要重试
        on_retry: 重试时的回调函数
    """

    last_error = None

    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)

        except exceptions as e:
            last_error = e

            should_retry = True

            if retry_if is not None:
                should_retry = retry_if(e)

            if not should_retry or attempt == retries:
                raise

            if on_retry is not None:
                on_retry(attempt, e)

            time.sleep(delay)

    raise last_error or RuntimeError("重试失败，未捕获到异常")
