import inspect
import json
import os
import random
import subprocess
from pathlib import Path

import psutil

# def get_network_drive_by_name(target_name):
#     level = 1  # 只取简略信息
#     resume = 0
#     entries, total, resume = win32net.NetUseEnum(None, level, resume)
#     for entry in entries:
#         local = entry.get('local')  # 本地盘符（如 Z:）
#         remote = entry.get('remote')  # 网络路径
#         if local and remote and target_name.lower() in remote.lower():
#             return local
#     return None


def turn_to_windows_path(filepath: str):
    return filepath.replace("/", "\\")


def ensure_dirs(path: str) -> str:
    """
    确保路径中所有文件夹都存在，若缺失则创建
    :param path: 只包含文件夹的路径
    :return: 完整路径（字符串）
    """
    os.makedirs(path, exist_ok=True)
    return os.path.abspath(path)


def jsonstr_to_dict(jsonostr: str) -> dict:
    return json.loads(jsonostr)


def is_file_locked(filepath: str) -> bool:
    """检测文件是否被占用"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"文件不存在: {filepath}")

    try:
        # 尝试以独占方式打开
        fd = os.open(filepath, os.O_RDWR | os.O_EXCL)
        os.close(fd)
        return False  # 能打开，说明没有被占用
    except OSError:
        return True  # 打不开，说明被占用


def pick_one(lst: list) -> any:
    if not lst:
        return None
    return random.choice(lst)


def launch_edge_with_default_profile():
    # 注意：msedge 要在 PATH 中，或者写全路径
    edge_path = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
    args = [edge_path, "--profile-directory=Default"]
    # 使用默认用户配置（默认不加 --user-data-dir 就会用系统的用户目录）
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    logger.info(f"Edge launched, PID = {proc.pid}")
    return proc.pid


def kill_process_by_pid(pid: int):
    try:
        proc = psutil.Process(pid)
        proc.terminate()  # 优雅结束
        proc.wait(timeout=5)  # 等待进程退出
        logger.info(f"进程 {pid} 已结束")
    except psutil.NoSuchProcess:
        logger.info(f"进程 {pid} 不存在")
    except Exception as e:
        logger.info(f"结束进程 {pid} 失败: {e}")


def split_files_adapt_windows(filenames: list[Path]) -> list[list[Path]]:
    """
    分割文件列表，以适配 Windows 文件选择器的限制

    ## param

    - filenames: 要分隔的文件列表

    ## return

    二维文件列表，由 Path 对象构成
    """
    STANDARD = 259
    filenames_str = [f'"{filename.name}"' for filename in filenames]

    if len(" ".join(filenames_str)) <= STANDARD:
        return [filenames]

    weights = [len(item.name) + 3 for item in filenames]
    weights[len(weights) - 1] -= 1

    file_groups = [[]]
    group_index = 0
    the_sum = 0

    for index, weight in enumerate(weights):
        the_sum += weight
        if the_sum > STANDARD:
            group_index += 1
            file_groups.append([])
            the_sum = weight
        file_groups[group_index].append(filenames[index])

    return file_groups


def search_folder(directoy: str, keyword: str) -> list:
    """
    从指定目录中搜索包含关键字的文件夹

    ## param

    - directory: 文件夹路径
    - keyword: 关键词

    ## return

    搜索结果列表，由Path元素构成
    """

    results = []
    base_dir = Path(directoy)
    for folder in base_dir.rglob(keyword):
        folder.is_dir() and results.append(folder)
    return results


def contains_any(s: str, keywords: list[str]) -> bool:
    """判断字符串 s 是否包含 keywords 列表中的任意一个元素"""
    return any(k in s for k in keywords)


def track(msg: str):
    frame = inspect.currentframe()
    caller = frame.f_back
    filename = os.path.basename(caller.f_code.co_filename)
    return f"[line: {caller.f_lineno}][{filename}][{caller.f_code.co_name}]: {msg}"
