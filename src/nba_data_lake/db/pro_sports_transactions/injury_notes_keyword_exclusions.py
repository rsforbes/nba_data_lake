from ...library.basedb import BaseDB
import aiofiles
from aiocsv import AsyncReader


class InjuryNotesKeywordExclusions(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(
            self, "prosports.transactions.injuries.notes.keyword.exclusions"
        )
        self._init_sql()

    async def drop(self) -> None:
        sql = 'DROP TABLE IF EXISTS "{}"'
        self.execute(sql.format(self._table))

    async def create(self) -> None:
        await self.execute(self._sql.table.format(self._table))

    async def load(self):
        data = []
        async with aiofiles.open(
            "/workspaces/nba_data_lake/src/nba_data_lake/db/data/injury.notes.keyword.exclusions.csv",
            mode="r",
        ) as f:
            async for row in AsyncReader(f):
                data.append(row)

        # Skip header row [1:]
        await self.executemany(self._sql.load.format(self._table), list(data)[1:])

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[],
            insert="",
            load='INSERT OR IGNORE INTO "{}"  VALUES (?)',
            table="""
                CREATE TABLE IF NOT EXISTS"{}" (
                "keyword" TEST,
                PRIMARY KEY("keyword"))
                """,
            triggers=[],
            select="SELECT keyword FROM {}",
        )
