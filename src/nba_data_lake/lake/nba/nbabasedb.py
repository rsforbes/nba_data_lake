from ...library.basedb import BaseDB


class NBABaseDB(BaseDB):
    def __init__(self, table) -> None:
        super().__init__(table)

    async def create(self) -> None:
        sql = 'CREATE TABLE IF NOT EXISTS "{}" ("data" TEXT)'
        await self.execute(sql.format(self._table))

    async def insert(self, *args) -> None:
        sql = 'INSERT OR REPLACE INTO "{}" (data) VALUES (?)'
        await self.execute(sql.format(self._table), *args)

    async def select(self) -> None:
        sql = 'SELECT FROM "{}"'
        await self.fetchall(sql.format(self._table))
