from urllib.parse import quote_plus

from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import Engine, Row
from sqlalchemy.orm import Session

DB_CONFIG = {
    "host": "ocean-mysql.mysql.rds.aliyuncs.com",
    "user": "rpaInternalEdit",
    "password": quote_plus("nmho4rpa2yJ@Oph"),
    "database": "cy_prod_scm_base_cy",
    "charset": "utf8mb4",
}


class ScmDb:
    def __init__(self):
        self.config = DB_CONFIG
        self.engine: Engine = self._create_engine()
        self.metadata = MetaData()

    def get_session(self) -> Session:
        """获取数据库会话"""
        return Session(self.engine)

    def _create_engine(self) -> Engine:
        return create_engine(
            "mysql+pymysql://{user}:{password}@{host}/{database}?charset={charset}&connect_timeout=32".format(
                **self.config
            ),
            echo=False,
        )

    def get_table(self, table_name: str) -> Table:
        """
        反射表结构（带缓存）
        """
        if table_name not in self.metadata.tables:
            Table(table_name, self.metadata, autoload_with=self.engine)
        return self.metadata.tables[table_name]

    def fetch_one_by_id(self, sku_id: str):
        """
        根据 id 查询单条记录
        """
        table = self.get_table("erp_dw_tiktok_msku_online")

        stmt = select(table).where(table.c.msku_id == sku_id)

        with self.engine.connect() as conn:
            result = conn.execute(stmt).first()
            return result

    def execute(self, stmt):
        """
        执行 insert / update / delete
        """
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def fetch_many_by_sku_ids(self, sku_ids: list[str]) -> dict[str, dict]:
        """
        批量查询 SCM 表，根据 sku_ids 返回 sku_id -> dict 映射
        """
        if not sku_ids:
            return {}

        table: Table = self.get_table("erp_dw_tiktok_msku_online")

        stmt = select(table).where(table.c["msku_id"].in_(sku_ids))

        with self.engine.connect() as conn:
            rows: list[Row] = conn.execute(stmt).fetchall()

        result_map = {row._mapping["msku_id"]: dict(row._mapping) for row in rows}
        return result_map

    def query_logistics_by_platform_order_no(
        self,
        platform_order_nos: list[str],
    ):
        if not platform_order_nos:
            return []

        logistics = self.get_table("erp_dw_order_logistics_info")
        items = self.get_table("erp_dw_order_items")
        orders = self.get_table("erp_dw_orders")

        stmt = (
            select(
                items.c.platform_order_no,
                logistics,
                orders.c.global_delivery_time,
            )
            .select_from(
                items.outerjoin(
                    logistics,
                    logistics.c.global_order_no == items.c.global_order_no,
                ).outerjoin(
                    orders,
                    orders.c.global_order_no == items.c.global_order_no,
                )
            )
            .where(items.c.platform_order_no.in_(platform_order_nos))
        )

        with self.engine.connect() as conn:
            return conn.execute(stmt).mappings().all()

    def query_logistics_by_global_order_no(
        self,
        global_order_nos: list[str],
    ):
        if not global_order_nos:
            return []

        logistics = self.get_table("erp_dw_order_logistics_info")

        stmt = select(logistics).where(
            logistics.c.global_order_no.in_(global_order_nos)
        )

        with self.engine.connect() as conn:
            return conn.execute(stmt).mappings().all()
