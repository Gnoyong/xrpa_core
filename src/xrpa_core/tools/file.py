import re
from pathlib import Path

# Windows 保留文件名
WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}


def sanitize_filename(name: str) -> str:
    """
    清理文件名，兼容 Windows / Linux
    """
    # 去除控制字符
    name = re.sub(r"[\x00-\x1f]", "", name)

    # 替换非法字符（Windows）
    name = re.sub(r'[<>:"/\\|?*]', "_", name)

    # 去掉结尾空格和点（Windows 不允许）
    name = name.rstrip(" .")

    # 防止空字符串
    if not name:
        name = "file"

    # 防止 Windows 保留名
    if name.upper() in WINDOWS_RESERVED_NAMES:
        name = f"_{name}"

    return name


def rename_file(
    file_path: str, new_name: str | None = None, new_suffix: str | None = None
) -> str:
    """
    修改文件名或后缀（自动处理非法字符）

    :param file_path: 原文件路径
    :param new_name: 新文件名（不带后缀）
    :param new_suffix: 新后缀（如 'txt' 或 '.txt'）
    :return: 新文件路径
    """
    p = Path(file_path)

    if not p.exists():
        raise FileNotFoundError(f"{file_path} 不存在")

    # 原始名称
    name = new_name if new_name else p.stem
    name = sanitize_filename(name)

    # 后缀处理
    if new_suffix:
        if not new_suffix.startswith("."):
            new_suffix = "." + new_suffix
    else:
        new_suffix = p.suffix

    new_path = p.with_name(name + new_suffix)

    p.rename(new_path)

    return str(new_path)
