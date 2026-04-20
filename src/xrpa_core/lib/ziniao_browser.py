import json
import os
import platform
import shutil
import subprocess
import time
import traceback
import uuid
import winreg

import requests
from DrissionPage import Chromium
from DrissionPage.common import By

from xrpa_core.core.logger import logger


def get_ziniao_exe():
    """
    获取紫鸟浏览器专业版在注册表中的DisplayIcon路径
    返回：DisplayIcon的字符串值，如果不存在则返回None
    """
    # 注册表路径
    key_path = r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\紫鸟浏览器专业版"

    try:
        # 打开注册表项
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0, winreg.KEY_READ)

        try:
            # 查询DisplayIcon的值
            value, value_type = winreg.QueryValueEx(key, "DisplayIcon")

            # 检查值类型是否为字符串（REG_SZ）
            if value_type == winreg.REG_SZ:
                return value
            else:
                logger.info(f"DisplayIcon的值类型不是字符串，类型为：{value_type}")
                return None

        except FileNotFoundError:
            logger.info("注册表项中不存在DisplayIcon值")
            return None

        finally:
            # 关闭注册表键
            winreg.CloseKey(key)

    except FileNotFoundError:
        logger.info("指定的注册表路径不存在")
        return None
    except PermissionError:
        logger.info("没有权限访问注册表，请以管理员身份运行")
        return None
    except Exception as e:
        logger.info(f"访问注册表时出错：{e}")
        return None


def get_configured_ziniao():
    return ZiniaoBrowser("尾号4270的公司16", "刘智勇", "j{UDmK=A", get_ziniao_exe())


class ZiniaoBrowser:
    """紫鸟浏览器自动化控制类 - DrissionPage版本"""

    def __init__(
        self,
        company: str,
        username: str,
        password: str,
        client_path: str,
        socket_port: int = 16851,
    ):
        """
        初始化紫鸟浏览器控制器

        Args:
            company: 公司名称
            username: 用户名
            password: 密码
            client_path: 客户端路径
            socket_port: 通信端口,默认16851
        """
        self.is_windows = platform.system() == "Windows"
        if not self.is_windows:
            raise OSError("webdriver/cdp只支持Windows和Mac操作系统")

        self.socket_port = socket_port
        self.client_path = client_path

        self.user_info = {
            "company": company,
            "username": username,
            "password": password,
        }

        self._initialized = False

    def initialize(self) -> bool:
        """
        初始化浏览器环境
        包括:终止旧进程、启动客户端、更新内核

        Returns:
            bool: 初始化是否成功
        """
        try:
            logger.info("=====终止旧进程=====")
            self._kill_process()

            logger.info("=====启动客户端=====")
            self._start_browser()

            logger.info("=====更新内核=====")
            self._update_core()

            self._initialized = True
            logger.info("=====初始化完成=====")
            return True
        except Exception as e:
            logger.exception(f"初始化失败: {e}")
            return False

    def _kill_process(self):
        """终止紫鸟客户端已启动的进程"""
        if self.is_windows:
            os.system("taskkill /f /t /im SuperBrowser.exe")
            time.sleep(3)

    def _start_browser(self):
        """启动客户端"""
        try:
            if self.is_windows:
                cmd = [
                    self.client_path,
                    "--run_type=web_driver",
                    "--ipc_type=http",
                    "--listen_ip=0.0.0.0",
                    f"--port={self.socket_port}",
                ]
            else:
                raise OSError("仅支持Windows系统")

            subprocess.Popen(cmd)
            time.sleep(5)
            logger.info("客户端启动成功")
        except Exception:
            logger.error("启动客户端失败: " + traceback.format_exc())
            raise

    def _update_core(self):
        """下载所有内核"""
        data = {
            "action": "updateCore",
            "requestId": str(uuid.uuid4()),
        }
        data.update(self.user_info)

        while True:
            result = self._send_http(data)
            if result is None:
                logger.info("等待客户端启动...")
                time.sleep(2)
                continue
            if result.get("statusCode") is None or result.get("statusCode") == -10003:
                logger.info("当前版本不支持此接口，请升级客户端")
                return
            elif result.get("statusCode") == 0:
                logger.info("更新内核完成")
                return
            else:
                logger.info(f"等待更新内核: {json.dumps(result)}")
                time.sleep(2)

    def _send_http(self, data: dict) -> dict | None:
        """
        发送HTTP请求与客户端通信

        Args:
            data: 请求数据

        Returns:
            响应结果字典
        """
        try:
            url = f"http://127.0.0.1:{self.socket_port}"
            response = requests.post(url, json.dumps(data).encode("utf-8"), timeout=120)
            return json.loads(response.text)
        except Exception as err:
            logger.info(f"HTTP请求失败: {err}")
            return None

    def get_browser_list(self) -> list[dict]:
        """
        获取店铺列表

        Returns:
            店铺信息列表
        """
        request_id = str(uuid.uuid4())
        data = {"action": "getBrowserList", "requestId": request_id}
        data.update(self.user_info)

        r = self._send_http(data)
        if str(r.get("statusCode")) == "0":
            return r.get("browserList", [])
        elif str(r.get("statusCode")) == "-10003":
            logger.error(f"登录错误: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("登录失败")
        else:
            logger.error(f"获取店铺列表失败: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("获取店铺列表失败")

    def open_store(self, store_info: str, **kwargs) -> dict:
        """
        打开店铺

        Args:
            store_info: 店铺ID或OAuth
            **kwargs: 可选参数
                - isWebDriverReadOnlyMode: 是否只读模式,默认0
                - isprivacy: 是否隐私模式,默认0
                - isHeadless: 是否无头模式,默认0
                - cookieTypeSave: Cookie保存类型,默认0
                - jsInfo: JS注入信息,默认""

        Returns:
            打开店铺的响应信息
        """
        if not self._initialized:
            raise Exception("请先调用initialize()方法初始化")

        request_id = str(uuid.uuid4())
        data = {
            "action": "startBrowser",
            "isWaitPluginUpdate": 0,
            "isHeadless": kwargs.get("isHeadless", 0),
            "requestId": request_id,
            "isWebDriverReadOnlyMode": kwargs.get("isWebDriverReadOnlyMode", 0),
            "cookieTypeLoad": 0,
            "cookieTypeSave": kwargs.get("cookieTypeSave", 0),
            "runMode": "1",
            "isLoadUserPlugin": False,
            "pluginIdType": 1,
            "privacyMode": kwargs.get("isprivacy", 0),
        }
        data.update(self.user_info)

        if store_info.isdigit():
            data["browserId"] = store_info
        else:
            data["browserOauth"] = store_info

        js_info = kwargs.get("jsInfo", "")
        if len(str(js_info)) > 2:
            data["injectJsInfo"] = json.dumps(js_info)

        r = self._send_http(data)
        if str(r.get("statusCode")) == "0":
            return r
        elif str(r.get("statusCode")) == "-10003":
            logger.error(f"登录错误: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("登录失败")
        else:
            logger.error(f"打开店铺失败: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("打开店铺失败")

    def get_store_by_name(self, store_name: str) -> dict | None:
        """
        根据店铺名称获取店铺信息

        Args:
            store_name: 店铺名称(支持模糊匹配)

        Returns:
            店铺信息字典,如果找不到返回None
        """
        if not store_name:
            raise Exception("store_name 是必填参数")

        stores = self.get_browser_list()

        # 精确匹配
        for store in stores:
            if store.get("browserName") == store_name:
                return store

        # 模糊匹配
        for store in stores:
            if store_name.lower() in store.get("browserName", "").lower():
                return store

        logger.info(f"未找到名称为 '{store_name}' 的店铺")
        return None

    def open_store_by_name(
        self, store_name: str, check_ip: bool = True, open_launcher: bool = True
    ) -> tuple[Chromium | None, int | None]:
        """
        根据店铺名称打开店铺并返回可控制的Chromium实例

        Args:
            store_name: 店铺名称(支持模糊匹配)
            check_ip: 是否检测IP,默认True
            open_launcher: 是否打开店铺主页,默认True

        Returns:
            Chromium实例,如果失败返回None
        """
        if not self._initialized:
            raise Exception("请先调用initialize()方法初始化")

        # 查找店铺
        store = self.get_store_by_name(store_name)
        if not store:
            return None, None

        # 打开店铺
        return self.open_and_control_store(store, check_ip, open_launcher)

    def close_store(self, browser_oauth: str) -> dict:
        """
        关闭店铺

        Args:
            browser_oauth: 店铺OAuth

        Returns:
            关闭店铺的响应信息
        """
        request_id = str(uuid.uuid4())
        data = {
            "action": "stopBrowser",
            "requestId": request_id,
            "duplicate": 0,
            "browserOauth": browser_oauth,
        }
        data.update(self.user_info)

        r = self._send_http(data)
        if str(r.get("statusCode")) == "0":
            return r
        elif str(r.get("statusCode")) == "-10003":
            logger.error(f"登录错误: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("登录失败")
        else:
            logger.error(f"关闭店铺失败: {json.dumps(r, ensure_ascii=False)}")
            raise Exception("关闭店铺失败")

    def close_store_by_name(self, store_name: str) -> bool:
        """
        根据店铺名称关闭店铺

        Args:
            store_name: 店铺名称(支持模糊匹配)

        Returns:
            bool: 是否成功关闭
        """
        if not self._initialized:
            raise Exception("请先调用initialize()方法初始化")

        # 查找店铺
        store = self.get_store_by_name(store_name)
        if not store:
            return False

        # 获取OAuth
        browser_oauth = store.get("browserOauth") or store.get("browserId")
        if not browser_oauth:
            logger.info(f"店铺 {store_name} 缺少OAuth信息")
            return False

        try:
            self.close_store(browser_oauth)
            logger.info(f"成功关闭店铺: {store_name}")
            return True
        except Exception as e:
            logger.error(f"关闭店铺 {store_name} 失败: {e}")
            return False

    @staticmethod
    def get_browser(port) -> Chromium:
        browser = Chromium(f"127.0.0.1:{port}")
        return browser

    def open_and_control_store(
        self, store_info: dict, check_ip: bool = True, open_launcher: bool = True
    ) -> tuple[Chromium | None, int | None]:
        """
        打开店铺并返回可控制的Chromium实例

        Args:
            store_info: 店铺ID或OAuth或店铺信息字典
            check_ip: 是否检测IP,默认True
            open_launcher: 是否打开店铺主页,默认True

        Returns:
            Chromium实例,如果失败返回None
        """
        if not self._initialized:
            raise Exception("请先调用initialize()方法初始化")

        # 如果传入的是字典,提取店铺标识
        if isinstance(store_info, dict):
            store_id = store_info.get("browserOauth") or store_info.get("browserId")
            store_name = store_info.get("browserName", "未知店铺")
        else:
            store_id = store_info
            store_name = store_info

        logger.info(f"=====打开店铺：{store_name}=====")

        try:
            # 打开店铺
            ret_json = self.open_store(store_id)
            logger.info(f"店铺打开成功: {ret_json}")

            # 获取店铺OAuth
            oauth = ret_json.get("browserOauth") or ret_json.get("browserId")

            # 获取Chromium浏览器实例
            browser = ZiniaoBrowser.get_browser(ret_json.get("debuggingPort"))
            if browser is None:
                logger.error(f"=====获取Browser失败，关闭店铺：{store_name}=====")
                self.close_store(oauth)
                return None, None

            # IP检测
            if check_ip:
                ip_check_url = ret_json.get("ipDetectionPage")
                if not ip_check_url:
                    logger.error("ip检测页地址为空，请升级紫鸟浏览器到最新版")
                    self.close_store(oauth)
                    return None, None

                logger.info("开始IP检测...")
                if self._check_ip(browser, ip_check_url):
                    logger.info("IP检测通过")
                else:
                    logger.error("IP检测不通过，请检查")
                    self.close_store(oauth)
                    return None, None

            # 打开店铺主页
            if open_launcher:
                launcher_page = ret_json.get("launcherPage")
                if launcher_page:
                    logger.info("打开店铺平台主页...")
                    tab = browser.latest_tab
                    tab.get(launcher_page)
                    time.sleep(6)

            logger.info(f"店铺 {store_name} 准备就绪")
            return browser, ret_json.get("debuggingPort")

        except Exception as e:
            logger.error(f"打开店铺失败: {e}")
            traceback.print_exc()
            return None, None

    def _check_ip(self, browser: Chromium, ip_check_url: str) -> bool:
        """
        检测IP是否正常

        Args:
            browser: Chromium实例
            ip_check_url: IP检测页地址

        Returns:
            bool: IP是否正常
        """
        try:
            tab = browser.latest_tab
            tab.get(ip_check_url)
            # 等待查找元素60秒
            success_button = tab.ele(
                (By.XPATH, '//button[contains(@class, "styles_btn--success")]'),
                timeout=60,
            )
            if success_button:
                logger.info("ip检测成功")
                return True
            else:
                logger.error("ip检测超时")
                return False
        except Exception:
            logger.error(f"ip检测异常: {traceback.format_exc()}")
            return False

    def delete_all_cache(self, cache_path: str | None = None):
        """
        删除所有店铺缓存

        Args:
            cache_path: 自定义缓存路径,如果为None则使用默认路径
        """
        if not self.is_windows:
            return

        if cache_path:
            cache_folder = os.path.join(cache_path, "SuperBrowser")
        else:
            local_appdata = os.getenv("LOCALAPPDATA")
            cache_folder = os.path.join(local_appdata, "SuperBrowser")

        if os.path.exists(cache_folder):
            shutil.rmtree(cache_folder)
            logger.info(f"缓存已删除: {cache_folder}")

    def exit_client(self):
        """关闭客户端"""
        data = {"action": "exit", "requestId": str(uuid.uuid4())}
        data.update(self.user_info)
        logger.info("关闭紫鸟客户端")
        self._send_http(data)

    def __enter__(self):
        """支持上下文管理器"""
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出时关闭客户端"""
        self.exit_client()
