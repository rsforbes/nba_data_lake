from ...library.basedb import BaseDB
from pro_sports_transactions import TransactionType
from typing import List
import json


class Transactions(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(self, table="lake.prosports.transactions")
        self._init_sql()

    async def create_table(self) -> None:
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def drop(self) -> None:
        await self.drop()

    async def insert(
        self, transactions: List, transaction_type: TransactionType
    ) -> None:
        transactions = [
            [
                json.dumps(transaction, ensure_ascii=False).encode("utf-8"),
                transaction_type.name,
            ]
            for transaction in transactions
        ]

        await self.executemany(
            self._sql.insert.format(self._table),
            transactions,
        )

    async def get_max_transaction_dates(self) -> None:
        sql = """
            SELECT
                MAX(json_extract(transaction_data,'$.Date')) as max_transaction_date, 
                transaction_type
            FROM "{}"
            GROUP BY transaction_type
            """
        return await self.fetchall(sql.format(self._table))

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[
                """
                CREATE UNIQUE INDEX IF NOT EXISTS"{}.unique"
                ON "{}" (
                    "transaction_data",
                    "transaction_type"
                )
                """,
                """
                CREATE INDEX IF NOT EXISTS"idx.{}.transaction_type" ON "{}" (
                    "transaction_type" ASC,
                    json_extract("transaction_data", '$.Date') DESC
                )
                """,
                """
                CREATE INDEX "idx.{}.transaction_date" 
                ON "{}" (
                    json_extract("transaction_data", '$.Date') DESC,
                    "transaction_type"	ASC
                )
                """,
            ],
            insert='INSERT OR IGNORE INTO "{}" (transaction_data, transaction_type) VALUES (?, ?)',
            load="",
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                    "transaction_id" INTEGER,
                    "transaction_data" TEXT,
                    "transaction_type" TEXT,
                    PRIMARY KEY("transaction_id" AUTOINCREMENT)
                )
                """,
            triggers=[],
            select="",
        )
