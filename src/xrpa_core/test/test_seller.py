from xrpa_core.command.order.fetch_seller_orders import FetchSellerOrdersCommand

# page, store = get_env_and_store("official")
# api = SellerAPI(page, store)
# print(api.get_export_records())

FetchSellerOrdersCommand().import_db(
    "official", r"C:\Users\Administrator\Desktop\All order-2026-03-17-00_13.csv"
)
