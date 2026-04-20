from xrpa_core.config import app_config
from xrpa_core.service.inventory_service import InventoryService

shop = app_config.fully_shops[0]
shop.id = "test"
InventoryService().import_inventory_file(
    shop, r"C:\Users\Administrator\Downloads\TIKTOK-Bafully_7628824729726732048.xlsx"
)
