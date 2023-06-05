from ..src.nba_data_lake.db.nba.nbabasedb import NBABaseDB
import aiosqlite


class CommonAllPlayers(NBABaseDB):
    def __init__(self) -> None:
        NBABaseDB.__init__("nba.commonallplayers")

    async def create(self):
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                f"""
                    CREATE TABLE IF NOT EXISTS "{self._table}" (
                    "person_id"	INTEGER,
                    "display_last_comma_first" TEXT,
                    "display_first_last" TEXT,
                    "rosterstatus" INTEGER,
                    "from_year"	INTEGER,
                    "to_year" TEXT,
                    "playercode" TEXT,
                    "player_slug" TEXT,
                    "team_id" INTEGER,
                    "team_city"	TEXT,
                    "team_name"	TEXT,
                    "team_abbreviation"	TEXT,
                    "team_code"	TEXT,
                    "team_slug"	TEXT,
                    "games_played_flag"	INTEGER,
                    "otherleague_experience_ch"	TEXT,
                    PRIMARY KEY("person_id"));
                """
            )
            await db.commit()
