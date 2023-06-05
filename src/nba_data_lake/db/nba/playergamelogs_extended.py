from .nbabasedb import NBABaseDB


class PlayerGameLogsExtended(NBABaseDB):
    def __init__(self) -> None:
        NBABaseDB.__init__(self, table="nba.playergamelogs.extended")
        self._init_sql()

    async def create(self):
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def load(self, season: str = "22022", source: str = "nba.playergamelogs"):
        await self.execute(
            self._sql.load.format(self._table, *(source for _ in range(3))), (season,)
        )

    def _init_sql(self):
        self._sql = self._SQL(
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                    season_id TEXT,
                    player_id INTEGER,
                    player_name TEXT,
                    team_name TEXT,
                    game_id TEXT,
                    game_date TEXT,
                    matchup TEXT,
                    linked_season_id TEXT,
                    linked_player_id TEXT,
                    linked_player_name TEXT,
                    linked_team_name TEXT,
                    linked_game_id TEXT,
                    linked_game_date TEXT,
                    linked_matchup TEXT,
                    days_between_games INTEGER,
                    PRIMARY KEY("player_id","game_id")
                    );
                """,
            insert="",
            indexes=[
                'CREATE INDEX IF NOT EXISTS "idx.{}.game_id" ON "{}" ("game_id")',
                'CREATE INDEX IF NOT EXISTS "idx.{}.game_date" ON "{}" ("game_date")',
                """
                CREATE INDEX IF NOT EXISTS "idx.{}.player_id" ON "{}" ("player_id" DESC,
                    "game_date" DESC,"game_id" DESC)
                """,
                """
                CREATE INDEX "idx.nba.playergamelogs.extended.season_id" ON 
                    "nba.playergamelogs.extended" ("season_id")
                """,
                """
                CREATE INDEX IF NOT EXISTS "idx.nba.playergamelogs.extended.player_name"
                 ON "nba.playergamelogs.extended" (
                    "player_name",
                    "season_id",
                    "game_date"
                )
                """
            ],
            triggers=[],
            load="""
                INSERT OR IGNORE INTO "{}"
                SELECT
                c.season_id,
                c.player_id,
                c.player_name,
                c.team_name,
                c.game_id,
                p.game_id AS prior_game_id,
                c.game_date,
                p.game_date AS prior_game_date,
                c.matchup,
                p.matchup AS prior_matchup,
                (julianday(c.game_date) - julianday(p.game_date) - 1) AS days_between_games,
                p.season_id AS linked_season_id,
                p.player_id AS linked_player_id,
                p.player_name AS linked_player_name,
                p.team_name AS linked_team_name
                FROM
                "{}" AS c
                LEFT JOIN
                "{}" AS p
                ON
                p.rowid = (
                    SELECT
                    MAX(rowid)
                    FROM
                    "{}"
                    WHERE
                    player_id = c.player_id
                    AND season_id = c.season_id
                    AND game_date < c.game_date
                )
                WHERE c.season_id = ?
                """,
            select="",
        )
