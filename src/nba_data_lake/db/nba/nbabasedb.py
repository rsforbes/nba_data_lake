from ...library.basedb import BaseDB
import aiosqlite


class NBABaseDB(BaseDB):
    def __init__(self, table) -> None:
        BaseDB.__init__(self, table)
        pass

    async def delete(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(f'DELETE FROM "{self._table}"')
            await db.commit()

    async def select(self) -> str:
        async with aiosqlite.connect(self._path) as db:
            cursor = await db.execute(f'SELECT FROM "{self._table}"')
            row = cursor.fetchone()
            return None if row is None else row[0]

    async def extract(self) -> str:
        pass
