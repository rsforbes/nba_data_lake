from .nbabasedb import NBABaseDB


class PlayerGameLogsPlayers(NBABaseDB):
    def __init__(self) -> None:
        NBABaseDB.__init__(self, table="nba.playergamelogs.players")
        self._init_sql()

    async def create(self):
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def load(self, season_id: str, source_table: str = "nba.playergamelogs"):
        await self.execute(
            self._sql.load.format(self._table, source_table), (season_id,)
        )

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[
                """
                CREATE INDEX IF NOT EXISTS"idx.{}.season_start_date"
                ON "{}" (
                    "season_start_date"	DESC,
                    "season_end_date"	DESC,
                    "player_name"	ASC,
                    "team_name"	ASC,
                    "player_id",
                    "team_id",
                    "season_id",
                    "season",
                    "season_type",
                    "player_start_date",
                    "player_end_date"
                )
                """,
                """
                CREATE UNIQUE INDEX IF NOT EXISTS"idx.{}.players.unique"
                ON "nba.playergamelogs.players" (
                    "player_id",
                    "team_id",
                    "season_id"
                )
                """,
            ],
            insert="",
            load="""
                INSERT OR REPLACE INTO "{}"
                SELECT
                    pgl.player_id,
                    pgl.team_id,
                    pgl.season_id,
                    pgl.player_name,
                    pgl.team_name,
                    s.season,
                    s.segment as season_type,
                    s.start as season_start_date,
                    s.end as season_end_date,
                    min(game_date) as player_start_date,
                    max(game_date) as player_end_date
                FROM "{}" pgl
                JOIN "nba.seasons" s
                ON s.season_id = pgl.season_id
                WHERE pgl.season_id = ?
                GROUP BY pgl.season_id, pgl.player_name, pgl.player_id, pgl.team_name, pgl.team_id
                """,
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                    "player_id"	TEXT,
                    "team_id"	NUMERIC,
                    "season_id"	TEXT,
                    "player_name"	TEXT,
                    "team_name"	TEXT,
                    "season"	TEXT,
                    "season_type"	INTEGER,
                    "season_start_date"	TEXT,
                    "season_end_date"	TEXT,
                    "player_start_date"	TEXT,
                    "player_end_date" TEXT
                );
                """,
            triggers=[],
            select="",
        )
