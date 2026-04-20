from xrpa_core.utils.env import get_creator_env


def get_env_and_store(store_id: str):
    from xrpa_core.config.config import get_config

    vm_service = get_creator_env()
    page = vm_service.get_env(store_id)
    config = get_config()
    store = config.get_store_by_id(store_id)
    assert store is not None
    return page, store
