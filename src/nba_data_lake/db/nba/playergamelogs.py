from .nbabasedb import NBABaseDB


class PlayerGameLogs(NBABaseDB):
    def __init__(self) -> None:
        NBABaseDB.__init__(self, "nba.playergamelogs")
        self._init_sql()

    async def create_table(self):
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def load(
        self,
        season: str,
        season_type: str,
        source_table: str = "lake.nba.stats.api.playergamelogs",
    ) -> None:
        await self.execute(
            self._sql.load.format(self._table, source_table), (season, season_type)
        )

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[
                """
                    CREATE INDEX IF NOT EXISTS "idx.{}.season_id" ON "{}" (
                        "season_id",
                        "player_id",
                        "team_id",
                        "team_name",
                        "player_name",
                        "game_date")
                """
            ],
            insert="",
            load="""
                INSERT OR IGNORE INTO "{}"
                    SELECT
                        json_extract(value, '$[0]'),
                        json_extract(value, '$[1]'),
                        json_extract(value, '$[2]'),
                        json_extract(value, '$[3]'),
                        json_extract(value, '$[4]'),
                        json_extract(value, '$[5]'),
                        json_extract(value, '$[6]'),
                        json_extract(value, '$[7]'),
                        json_extract(value, '$[8]'),
                        json_extract(value, '$[9]'),
                        json_extract(value, '$[10]'),
                        json_extract(value, '$[11]'),
                        json_extract(value, '$[12]'),
                        json_extract(value, '$[13]'),
                        json_extract(value, '$[14]'),
                        json_extract(value, '$[15]'),
                        json_extract(value, '$[16]'),
                        json_extract(value, '$[17]'),
                        json_extract(value, '$[18]'),
                        json_extract(value, '$[19]'),
                        json_extract(value, '$[20]'),
                        json_extract(value, '$[21]'),
                        json_extract(value, '$[22]'),
                        json_extract(value, '$[23]'),
                        json_extract(value, '$[24]'),
                        json_extract(value, '$[25]'),
                        json_extract(value, '$[26]'),
                        json_extract(value, '$[27]'),
                        json_extract(value, '$[28]'),
                        json_extract(value, '$[29]'),
                        json_extract(value, '$[30]'),
                        json_extract(value, '$[31]')
                FROM "{}",
                json_each(data,'$.resultSets[0].rowSet')
                WHERE season == ?
                AND season_type == ?
                """,
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                    "season_id"	TEXT NO CASE,
                    "player_id"	NUMERIC,
                    "player_name"	TEXT NO CASE,
                    "team_id"	INTEGER,
                    "team_abbreviation"	TEXT NO CASE,
                    "team_name"	TEXT NO CASE,
                    "game_id"	TEXT NO CASE,
                    "game_date"	TEXT NO CASE,
                    "matchup"	TEXT NO CASE,
                    "wl"	TEXT NO CASE,
                    "min"	INTEGER,
                    "fgm"	INTEGER,
                    "fga"	INTEGER,
                    "fg_pct"	INTEGER,
                    "fg3m"	INTEGER,
                    "fg3a"	INTEGER,
                    "fg3_pct"	INTEGER,
                    "ftm"	INTEGER,
                    "fta"	INTEGER,
                    "ft_pct"	INTEGER,
                    "oreb"	INTEGER,
                    "dreb"	INTEGER,
                    "reb"	INTEGER,
                    "ast"	INTEGER,
                    "stl"	INTEGER,
                    "blk"	INTEGER,
                    "tov"	INTEGER,
                    "pf"	INTEGER,
                    "pts"	INTEGER,
                    "plus_minus"	INTEGER,
                    "fantasy_pts"	INTEGER,
                    "video_available"	INTEGER,
                    PRIMARY KEY("player_id","game_id")
                )
                """,
            triggers=[],
            select='SELECT * FROM "{}"',
        )
