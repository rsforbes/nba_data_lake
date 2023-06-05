from ..library.basedb import BaseDB
from typing import Dict
import aiofiles
from aiocsv import AsyncReader


class OSICS(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(self, table="nba.seasons")
        self._init_sql()

    async def create_table(self):
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def load(self):
        seasons = []
        async with aiofiles.open(
            "/workspaces/nba_data_lake/src/nba_data_lake/db/data/nba.seasons.csv",
            mode="r",
        ) as f:
            async for row in AsyncReader(f):
                seasons.append(row)

        # Skip header row on nba.seasons.csv
        await self.executemany(self._sql.load.format(self._table), list(seasons)[1:])

    async def select(self) -> Dict:
        return await self.fetchall(self._sql.select.format(self._table))

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[
                'CREATE INDEX IF NOT EXISTS "idx.{}.season" ON "{}" (season)',
                """
                CREATE INDEX IF NOT EXISTS "idx.{}.season_id" ON "{}" ("season_id",
                    "season","segment","start","end")
                """,
                """
                CREATE INDEX IF NOT EXISTS "idx.{}.season_type" ON "{}" (segment)
                """,
            ],
            load='INSERT OR REPLACE INTO "{}" VALUES (?,?,?,?,?,?)',
            table="""
                    CREATE TABLE IF NOT EXISTS "nba.seasons" (
                        "season_id"	TEXT NOT NULL,
                        "season"	TEXT,
                        "start"	TEXT,
                        "end"	TEXT,
                        "segment"	TEXT,
                        "notes"	TEXT,
                        PRIMARY KEY("season_id")
                    )
                """,
            triggers=[],
            select='SELECT * FROM "{}"',
        )
