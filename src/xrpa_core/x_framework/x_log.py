import json
import uuid
from datetime import datetime
from pathlib import Path

from xrpa_core.x_framework.x_db import DataSource, Log, LogService


def log(
    pid: str, result_dict: dict, yingdao_logs: str, process_dict: dict, status: str
):
    if not pid or pid == "":
        raise Exception("pid 不可为空")

    result_json = json.dumps(result_dict, ensure_ascii=False)
    process_json = json.dumps(process_dict, ensure_ascii=False)
    ds = DataSource()
    log_service = LogService(ds)
    log = Log(
        id=uuid.uuid4().hex,
        pid=pid,
        result_json=result_json,
        yingdao_logs=yingdao_logs,
        process_json=process_json,
        date=datetime.now(),
        status=status,
    )
    # logger.info(f"[x_log] 记录日志至数据库：{log.to_dict()}")
    log_service.create(log)


def load_progress(filename: str):
    """
    从指定 JSON 文件加载进度数据
    :param filename: 日志文件路径
    :return: 进度数据（dict 或其他）
    """
    file_path = Path(filename)

    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {filename}")

    with open(file_path, encoding="utf-8") as f:
        progress = json.load(f)

    glv["x_progress_log"] = progress


def save_progress():
    from platformdirs import user_cache_dir
    from xbot_extensions.official_account.get_uuid import get_uuid

    uuid = get_uuid()
    progress = glv["x_progress_log"]
    datetime_text = datetime.now().strftime("%Y%m%d%H%M%S")

    cache_dir = Path(user_cache_dir("xbot"))
    cache_dir.mkdir(parents=True, exist_ok=True)

    filename = cache_dir / f"log_{uuid}_{datetime_text}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)

    return filename
