import os


def get_downloads_dir() -> str:
    import winreg

    key = winreg.OpenKey(
        winreg.HKEY_CURRENT_USER,
        r"Software\Microsoft\Windows\CurrentVersion\Explorer\User Shell Folders",
    )
    try:
        value, _ = winreg.QueryValueEx(key, "{374DE290-123F-4565-9164-39C4925E467B}")
        # 处理环境变量如 %USERPROFILE%
        return os.path.expandvars(value)
    finally:
        winreg.CloseKey(key)
