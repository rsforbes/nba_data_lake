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
                    "display_last_comma_first" TEXT NO CASE,
                    "display_first_last" TEXT NO CASE,
                    "rosterstatus" INTEGER,
                    "from_year"	INTEGER,
                    "to_year" TEXT NO CASE,
                    "playercode" TEXT NO CASE,
                    "player_slug" TEXT NO CASE,
                    "team_id" INTEGER,
                    "team_city"	TEXT NO CASE,
                    "team_name"	TEXT NO CASE,
                    "team_abbreviation"	TEXT NO CASE,
                    "team_code"	TEXT NO CASE,
                    "team_slug"	TEXT NO CASE,
                    "games_played_flag"	INTEGER,
                    "otherleague_experience_ch"	TEXT NO CASE,
                    PRIMARY KEY("person_id"));
                """
            )
            await db.commit()
