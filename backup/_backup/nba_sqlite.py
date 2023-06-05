from src.api.nba.season import Season
from pathlib import Path
from typing import List
import aiosqlite
import json as js


class NBA_Data:
    _dict = {}

    def __init__(self, json: str) -> None:
        self._dict = dict(js.loads(json))

    @property
    def raw(self) -> str:
        return js.dumps(self._dict)

    @property
    def resource(self):
        return js.dumps(self._dict["resource"])

    @property
    def parameters(self) -> str:
        return js.dumps(self._dict["parameters"])

    @property
    def result_sets(self) -> str:
        return js.dumps(self._dict["resultSets"])


class NBA_SQLite:
    nba_sqlite = Path.cwd().joinpath("nba.sqlite")

    def __init__(self, path: Path):
        self._path = path

    async def drop_table(self, resource) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(f"DROP TABLE IF EXISTS {resource};")
            await db.commit()

    async def create_table(self, resource) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                f"CREATE TABLE IF NOT EXISTS {resource} (\
                    resource TEXT, \
                    parameters TEXT, \
                    resultSets TEXT, \
                    raw TEXT, \
                    date_created TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))),\
                    date_modified TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))));"
            )
            await db.commit()

    async def write_data(self, data: NBA_Data) -> None:
        try:
            async with aiosqlite.connect(self._path) as db:
                await db.execute(
                    f"INSERT INTO {data.resource} \
                        (resource, parameters, resultSets, raw) \
                        values (?,?,?,?)",
                    (
                        data.resource,
                        data.parameters,
                        data.result_sets,
                        data.raw,
                    ),
                )
                await db.commit()
        except Exception:
            raise

    async def get_data(self, resource: str) -> List[NBA_Data]:
        try:
            async with aiosqlite.connect(self._path) as db:
                cursor = await db.execute(f"SELECT raw FROM {resource}")
                rows = await cursor.fetchall()
                return rows if len(rows) == 0 else [x for x in rows[0]]
        except Exception:
            raise

    async def get_playercareerstats(self) -> List[int]:
        try:
            async with aiosqlite.connect(self._path) as db:
                cursor = await db.execute(
                    "SELECT json_extract(pcs.parameters,'$.PlayerID') \
                        AS player_id \
                        FROM playercareerstats AS pcs"
                )
                rows = await cursor.fetchall()
                return [x[0] for x in rows]
        except Exception:
            raise


class SeasonDB:
    def __init__(self, path: Path):
        self._path = path

    async def get_season(self) -> dict:
        try:
            async with aiosqlite.connect(self._path) as db:
                cursor = await db.execute(
                    "SELECT value FROM seasons, json_each(data, '$[0].rowSet')"
                )
                rows = await cursor.fetchall()
                for row in rows:
                    print(row[0])

                # print(data_list)
                # return rows if len(rows) == 0 else [Season(list(r)) for r in rows]

        except Exception:
            raise
