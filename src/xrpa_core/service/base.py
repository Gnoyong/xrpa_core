from xrpa_core.db import default_dm
from xrpa_core.db.models import DatabaseManager


class BaseService:
    """
    Base service class for Temu Auto.
    """

    def __init__(self, dm: DatabaseManager | None = None):
        if dm is None:
            dm = default_dm

        self.dm = dm

    def _get_session(self):
        return self.dm.get_session()
