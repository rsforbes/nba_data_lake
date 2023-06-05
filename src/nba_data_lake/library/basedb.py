from collections import namedtuple
from typing import List
import aiosqlite
import re


class BaseDB:
    _path = "/ext/sqlite/nba.sqlite"

    _SQL = namedtuple(
        "SQL", ["indexes", "insert", "load", "table", "triggers", "select"]
    )

    def __init__(self, table) -> None:
        self._table = self.validate_table_name(table)

    def dict_factory(self, cursor: aiosqlite.Cursor, row):
        fields = [column[0] for column in cursor.description]
        return {key: value for key, value in zip(fields, row)}

    async def drop(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(f'DROP TABLE "{self._table}"')
            await db.commit()

    async def execute(self, sql, *args) -> None:
        async with aiosqlite.connect(self._path) as db:
            if len(args) == 0:
                await db.execute(sql)
            else:
                await db.execute(sql, *args)
            await db.commit()

    async def executemany(self, sql, data: List) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.executemany(sql, data)
            await db.commit()

    async def fetchall(self, sql, *args):
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = self.dict_factory
            cursor = await db.execute(sql, *args)
            rows = await cursor.fetchall()
            return rows

    def validate_table_name(self, table_name) -> str:
        # Use a regular expression to validate the table name format securely
        # Allow alphanumeric characters, underscores, and periods without length limit
        # Example: Must start with a letter and allow only safe characters
        if not re.match(r"^[a-zA-Z][a-zA-Z0-9_.]*$", table_name):
            raise ValueError("Invalid table name")
        return table_name
