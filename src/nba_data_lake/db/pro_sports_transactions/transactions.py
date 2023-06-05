from ...library.basedb import BaseDB
import aiosqlite


class Transactions(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(self, "prosports.transactions")
        self._init_sql()

    async def drop(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(f'DROP TABLE IF EXISTS "{self._table}"')
            await db.commit()

    async def create(self) -> None:
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def load(self, source_table: str = "lake.prosports.transactions") -> None:
        await self.execute(self._sql.load.format(self._table, source_table))

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[],
            insert="",
            load="""
            INSERT OR IGNORE INTO "{}"
            SELECT
                transaction_id,
                json_extract(transaction_data,'$.Date') AS "transaction_date",
                json_extract(transaction_data,'$.Team') AS team_name,
                trim(replace(json_extract(transaction_data,'$.Acquired'),'•','')) AS acquired,
                trim(replace(json_extract(transaction_data,'$.Relinquished'),'•','')) AS relinquished,
                json_extract(transaction_data,'$.Notes') as notes,
                transaction_type
            FROM "{}"
                """,
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                "transaction_id" INTEGER,
                "transaction_date" TEXT,
                "team_name"	TEXT,
                "acquired" TEXT,
                "relinquished" TEXT,
                "notes"	TEXT,
                "transaction_type" TEXT,
                PRIMARY KEY("transaction_id"))
                """,
            triggers=[],
            select="",
        )
