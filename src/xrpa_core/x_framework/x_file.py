import time
from pathlib import Path


def main(args):
    pass


def wait_file(dir_path, filename, timeout=30, interval=0.5):
    """
    等待目录中出现指定文件名

    :param dir_path: 目录路径（str / Path）
    :param filename: 文件名（不含路径）
    :param timeout: 最大等待时间（秒）
    :param interval: 轮询间隔（秒）
    :return: Path
    """
    dir_path = Path(dir_path)
    target = dir_path / filename

    start = time.time()
    while time.time() - start < timeout:
        if target.exists():
            return target
        time.sleep(interval)

    raise TimeoutError(f"{timeout}s 内未等到文件：{filename}")
