import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from DrissionPage import ChromiumPage
from ping3 import ping

from xrpa_core.core.logger import logger
from xrpa_core.dao.kv_dao import kv_dao
from xrpa_core.db.models import DatabaseManager
from xrpa_core.entity.zb_credential import ZbCredential
from xrpa_core.hyperv_manager import hyperv_manager
from xrpa_core.lib.ziniao_browser_v2 import ZiniaoBrowserV2
from xrpa_core.service.base import BaseService


class VmEnvService(BaseService):
    def __init__(self, vm_name: str, zb_credential: ZbCredential, dm: DatabaseManager):
        self.name = vm_name
        hyperv_manager.start_vm(vm_name)
        host = hyperv_manager.get_vm_ip(vm_name)
        if not host:
            raise RuntimeError(f"无法获取虚拟机 {vm_name} 的 IP 地址")

        self.host = host

        self._host_lock = threading.Lock()
        self.zb = ZiniaoBrowserV2(
            zb_credential.company,
            zb_credential.username,
            zb_credential.password,
            r"C:/Users/xen/SuperBrowser/5.290.1.15/SuperBrowser.exe",
            self.host,
        )
        super().__init__(dm)

    def _is_chrome_debug_port_available(self, port: int) -> bool:
        """测试 Chrome DevTools Protocol 端口是否可用"""
        try:
            resp = requests.get(f"http://{self.host}:{port}/json/version", timeout=3)
            return resp.status_code == 200
        except Exception:
            return False

    def get_env(self, ziniao_name: str) -> ChromiumPage:
        def _safe_get_env(ziniao_name) -> ChromiumPage:
            port = kv_dao.get(session, f"{ziniao_name}_port")

            if not port or not port.value:
                raise RuntimeError(f"未找到 {ziniao_name} 的端口配置")

            if not self._is_chrome_debug_port_available(int(port.value)):
                raise RuntimeError(
                    f"{ziniao_name} 的 Chrome 调试端口 {port.value} 不可用"
                )

            return ChromiumPage(f"{self.host}:{port.value}")

        # 这里可以添加获取指定店铺环境变量的逻辑
        with self.dm.get_session() as session:
            for i in range(3):  # 最多尝试 3 次
                try:
                    return _safe_get_env(ziniao_name)
                except RuntimeError as e:
                    logger.warning(
                        f"尝试获取 {ziniao_name} 环境失败（第 {i + 1} 次）：{e}"
                    )
                    # 尝试启动环境
                    self.launch([ziniao_name])
                    session.commit()

        raise RuntimeError(f"无法获取 {ziniao_name} 的环境")

    def _launch_single_store(self, shop_name: str) -> int | None:
        """启动单个店铺环境

        Returns:
            int | None: 端口号
        """
        logger.info(f"正在启动 {shop_name} 的环境...")
        try:
            _, port = self.zb.open_store_by_name(shop_name, False)
            logger.info(f"{shop_name} 的环境已启动，端口号：{port}")
            return port
        except Exception as e:
            logger.error(f"启动 {shop_name} 异常: {e}")
            return None

    def launch(self, store_names: list[str]):
        # 筛选需要启动的店铺
        if not store_names:
            logger.info("没有需要启动的店铺")
            return

        # 并发启动所有店铺
        results: dict[str, int | None] = {}
        with ThreadPoolExecutor(max_workers=len(store_names)) as executor:
            futures = {
                executor.submit(self._launch_single_store, shop_name): shop_name
                for shop_name in store_names
            }

            for future in as_completed(futures):
                shop_name = futures[future]
                result = future.result()
                results[shop_name] = result

        # 将成功的端口写入数据库
        with self._get_session() as session:
            for shop_name, port in results.items():
                if port:
                    kv_dao.set(session, f"{shop_name}_port", port)
            session.commit()

    @staticmethod
    def _is_host_available(host: str) -> bool:
        try:
            delay = ping(host, timeout=1)
            return delay is not None
        except Exception:
            return False
