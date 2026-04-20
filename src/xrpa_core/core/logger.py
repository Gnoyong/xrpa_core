import sys

from loguru import logger as _logger


def format_with_store(record):
    extra = record["extra"]

    prefix = extra.get("prefix") or (
        f"[{extra['store_name']}]" if extra.get("store_name") else None
    )

    if prefix:
        prefix_part = f"{prefix} | "
    else:
        prefix_part = ""

    return (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
        "| <level>{level}</level> "
        "| "
        f"{prefix_part}"
        "{message}\n"
    )


_logger.remove()

# 控制台输出 - 使用动态格式
_logger.add(
    sys.stdout,
    level="INFO",
    format=format_with_store,
)

# 文件日志
_logger.add(
    "logs/app_{time:YYYY-MM-DD}.log",
    level="INFO",
    rotation="10 MB",  # 超过 10MB 自动切割
    retention="14 days",  # 保留 14 天
    compression="zip",  # 旧日志压缩
    encoding="utf-8",
    format=lambda record: (
        format_with_store(record)
        .replace("<green>", "")
        .replace("</green>", "")
        .replace("<level>", "")
        .replace("</level>", "")
    ),
)

logger = _logger

# Define the configuration constants.
# WEBHOOK_ID = "123456790"
# WEBHOOK_TOKEN = "abc123def456"

# notifier = apprise.Apprise()
# notifier.add(f"discord://{WEBHOOK_ID}/{WEBHOOK_TOKEN}")

# logger.add(notifier.notify, level="ERROR", filter={"apprise": False})
