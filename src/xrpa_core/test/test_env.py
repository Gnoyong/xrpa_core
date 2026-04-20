from concurrent.futures import ThreadPoolExecutor, as_completed

from xrpa_core.config import app_config
from xrpa_core.core import logger
from xrpa_core.service.inventory_service import InventoryService


def export_shop_inventory(shop):
    logger.info(f"正在导出 {shop.full_name} 的库存文件...")
    InventoryService().fetch_inventory(shop)


def run_export_inventory_concurrent(max_workers: int = 3):
    shops = [shop for shop in app_config.fully_shops]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_shop = {
            executor.submit(export_shop_inventory, shop): shop for shop in shops
        }

        for future in as_completed(future_to_shop):
            shop = future_to_shop[future]
            try:
                future.result()
            except Exception as e:
                logger.exception(f"导出失败 | shop={shop.name} | {e}")


run_export_inventory_concurrent(max_workers=3)
