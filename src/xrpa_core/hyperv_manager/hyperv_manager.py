"""
Hyper-V 虚拟机管理工具类

通过 PowerShell 命令控制 Hyper-V 虚拟机的启停、状态查询等操作。
"""

import base64
import ctypes
import ipaddress
import os
import subprocess
import tempfile
from enum import Enum

from xrpa_core.core.logger import logger


class VMState(Enum):
    """虚拟机状态枚举"""

    OFF = "Off"
    STARTING = "Starting"
    RUNNING = "Running"
    PAUSED = "Paused"
    SAVED = "Saved"
    STOPPING = "Stopping"
    OTHER = "Other"

    @classmethod
    def from_str(cls, state_str: str) -> "VMState":
        """从字符串转换为枚举值"""
        state_map = {
            "Off": cls.OFF,
            "Starting": cls.STARTING,
            "Running": cls.RUNNING,
            "Paused": cls.PAUSED,
            "Saved": cls.SAVED,
            "Stopping": cls.STOPPING,
        }
        return state_map.get(state_str, cls.OTHER)


class HyperVManager:
    """Hyper-V 虚拟机管理器"""

    def __init__(self):
        self._check_hyperv_available()

    def _check_hyperv_available(self) -> None:
        """检查 Hyper-V 是否可用"""
        try:
            self._run_powershell("Get-VMHost", timeout=30, ignore_error=False)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("检查 Hyper-V 状态超时") from exc
        except FileNotFoundError as exc:
            raise RuntimeError(
                "PowerShell 未找到，请确保在 Windows 系统上运行"
            ) from exc
        except RuntimeError as exc:
            raise RuntimeError(
                "Hyper-V 不可用，请确保已启用 Hyper-V 功能并以管理员权限运行。\n"
                f"错误信息: {exc}"
            ) from exc

    @staticmethod
    def _run_powershell(
        command: str, timeout: int = 60, ignore_error: bool = False
    ) -> str:
        """执行 PowerShell 命令"""
        if HyperVManager._is_running_as_admin():
            # logger.info(f"执行 PowerShell 命令: {command}")
            result = subprocess.run(
                ["powershell", "-Command", command],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            return_code = result.returncode
        else:
            logger.info("当前未以管理员权限运行，通过 UAC 提权执行 PowerShell 命令")
            return_code, stdout, stderr = HyperVManager._run_powershell_as_admin(
                command, timeout
            )

        # 精确处理“找不到虚拟机”
        vm_not_found_keywords = [
            "was unable to find a virtual machine with name",
            "找不到名称为",
        ]
        if stderr:
            if ignore_error and any(k in stderr for k in vm_not_found_keywords):
                return ""
            raise RuntimeError(f"PowerShell 错误输出: {stderr}")

        if return_code != 0:
            if ignore_error:
                return ""
            raise RuntimeError(f"PowerShell 命令执行失败: {stderr}")
        return stdout

    @staticmethod
    def _is_running_as_admin() -> bool:
        """判断当前进程是否管理员权限运行。"""
        try:
            return bool(ctypes.windll.shell32.IsUserAnAdmin())
        except (AttributeError, OSError):
            return False

    @staticmethod
    def _ps_single_quote_escape(value: str) -> str:
        """PowerShell 单引号字符串转义。"""
        return value.replace("'", "''")

    @staticmethod
    def _run_powershell_as_admin(command: str, timeout: int) -> tuple[int, str, str]:
        """在非管理员上下文中通过 UAC 提权执行 PowerShell 命令。"""
        stdout_fd, stdout_path = tempfile.mkstemp(suffix=".stdout.txt")
        stderr_fd, stderr_path = tempfile.mkstemp(suffix=".stderr.txt")
        os.close(stdout_fd)
        os.close(stderr_fd)

        try:
            escaped_command = HyperVManager._ps_single_quote_escape(command)
            escaped_stdout_path = HyperVManager._ps_single_quote_escape(stdout_path)
            escaped_stderr_path = HyperVManager._ps_single_quote_escape(stderr_path)

            elevated_command = "\n".join(
                [
                    "$ErrorActionPreference = 'Stop'",
                    f"$stdoutPath = '{escaped_stdout_path}'",
                    f"$stderrPath = '{escaped_stderr_path}'",
                    f"$command = '{escaped_command}'",
                    "try {",
                    "    $output = @(Invoke-Expression $command 2>&1)",
                    "    $exitCode = if ($LASTEXITCODE -is [int]) { $LASTEXITCODE } else { 0 }",
                    "    [System.IO.File]::WriteAllText(",
                    "        $stdoutPath,",
                    "        (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine),",
                    "        [System.Text.UTF8Encoding]::new($false)",
                    "    )",
                    "    exit $exitCode",
                    "}",
                    "catch {",
                    "    [System.IO.File]::WriteAllText(",
                    "        $stderrPath,",
                    "        (($_ | Out-String).Trim()),",
                    "        [System.Text.UTF8Encoding]::new($false)",
                    "    )",
                    "    exit 1",
                    "}",
                ]
            )
            encoded_command = base64.b64encode(
                elevated_command.encode("utf-16le")
            ).decode("ascii")

            elevate_script = (
                "$ErrorActionPreference = 'Stop';"
                "$p = Start-Process -FilePath 'powershell' -Verb RunAs -Wait -PassThru "
                f"-ArgumentList @('-NoProfile','-NonInteractive','-EncodedCommand','{encoded_command}');"
                "exit $p.ExitCode"
            )

            wrapper_result = subprocess.run(
                [
                    "powershell",
                    "-NoProfile",
                    "-NonInteractive",
                    "-Command",
                    elevate_script,
                ],
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )

            with open(stdout_path, encoding="utf-8", errors="ignore") as f:
                stdout = f.read().strip()
            with open(stderr_path, encoding="utf-8", errors="ignore") as f:
                elevated_stderr = f.read().strip()

            wrapper_stderr = wrapper_result.stderr.strip()
            if elevated_stderr and wrapper_stderr:
                stderr = f"{elevated_stderr}\n{wrapper_stderr}"
            else:
                stderr = elevated_stderr or wrapper_stderr

            return wrapper_result.returncode, stdout, stderr
        finally:
            if os.path.exists(stdout_path):
                os.remove(stdout_path)
            if os.path.exists(stderr_path):
                os.remove(stderr_path)

    def get_vm_state(self, name: str) -> VMState | None:
        """
        获取虚拟机状态

        Args:
            name: 虚拟机名称

        Returns:
            虚拟机状态，不存在则返回 None
        """
        # 用双引号包裹 VM 名称，兼容中文和特殊字符
        command = f'Get-VM -Name "{name}" | Select-Object -ExpandProperty State'
        output = self._run_powershell(command, ignore_error=False)
        if not output:
            return None
        return VMState.from_str(output)

    def get_vm_ip(self, name: str, prefer_ipv4: bool = True) -> str | None:
        """
        根据虚拟机名称获取 IP 地址

        Args:
            name: 虚拟机名称
            prefer_ipv4: 是否优先返回 IPv4 地址

        Returns:
            IP 地址，不存在或无法获取时返回 None
        """
        state = self.get_vm_state(name)
        if state is None:
            logger.error(f"虚拟机不存在: {name}")
            return None

        command = f"Get-VMNetworkAdapter -VMName '{name}' -ErrorAction SilentlyContinue | Select-Object -ExpandProperty IPAddresses"
        output = HyperVManager._run_powershell(command, ignore_error=False)
        if not output:
            return None

        ip_candidates: list[tuple[str, ipaddress._BaseAddress]] = []
        for line in output.splitlines():
            ip_str = line.strip()
            if not ip_str:
                continue
            try:
                ip_obj = ipaddress.ip_address(ip_str)
            except ValueError:
                continue
            ip_candidates.append((ip_str, ip_obj))

        if not ip_candidates:
            return None

        if prefer_ipv4:
            for ip_str, ip_obj in ip_candidates:
                if ip_obj.version == 4:
                    return ip_str

        return ip_candidates[0][0]

    def start_vm(self, name: str) -> bool:
        """
        启动虚拟机

        Args:
            name: 虚拟机名称

        Returns:
            是否启动成功
        """
        # logger.info(f"启动虚拟机: {name}")
        state = self.get_vm_state(name)
        if state is None:
            logger.error(f"虚拟机不存在: {name}")
            return False
        if state == VMState.RUNNING:
            logger.info(f"虚拟机 {name} 正在在运行中")
            return True

        self._run_powershell(f"Start-VM -Name '{name}'")
        return True

    def stop_vm(self, name: str, force: bool = False) -> bool:
        """
        停止虚拟机

        Args:
            name: 虚拟机名称
            force: 是否强制关闭（相当于断电）

        Returns:
            是否停止成功
        """
        # logger.info(f"停止虚拟机: {name} (强制: {force})")
        state = self.get_vm_state(name)
        if state is None:
            logger.error(f"虚拟机不存在: {name}")
            return False
        if state == VMState.OFF:
            logger.info(f"虚拟机 {name} 已经处于关闭状态")
            return True

        if force:
            self._run_powershell(f"Stop-VM -Name '{name}' -TurnOff")
        else:
            self._run_powershell(f"Stop-VM -Name '{name}'")
        return True

    def save_vm(self, name: str) -> bool:
        """
        保存虚拟机状态（休眠）

        Args:
            name: 虚拟机名称

        Returns:
            是否保存成功
        """
        # logger.info(f"保存虚拟机状态: {name}")
        state = self.get_vm_state(name)
        if state is None:
            logger.error(f"虚拟机不存在: {name}")
            return False
        if state == VMState.SAVED:
            logger.info(f"虚拟机 {name} 已经处于保存状态")
            return True

        self._run_powershell(f"Save-VM -Name '{name}'")
        return True

    def pause_vm(self, name: str) -> bool:
        """
        暂停虚拟机

        Args:
            name: 虚拟机名称

        Returns:
            是否暂停成功
        """
        # logger.info(f"暂停虚拟机: {name}")
        state = self.get_vm_state(name)
        if state is None:
            logger.error(f"虚拟机不存在: {name}")
            return False
        if state == VMState.PAUSED:
            logger.info(f"虚拟机 {name} 已经处于暂停状态")
            return True

        self._run_powershell(f"Suspend-VM -Name '{name}'")
        return True
