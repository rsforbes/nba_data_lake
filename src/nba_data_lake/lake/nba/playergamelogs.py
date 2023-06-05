from .nbabasedb import NBABaseDB
from nba_api.stats.library.parameters import SeasonTypeAllStar


class PlayerGameLogs(NBABaseDB):
    def __init__(self) -> None:
        super().__init__("lake.nba.stats.api.playergamelogs")

    async def create(self) -> None:
        # Create Table
        sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
                "data" TEXT,
                "season" TEXT,
                "season_type" TEXT,
                "total_player_games" INTEGER,
                CONSTRAINT "uq_{}" UNIQUE("season", "season_type")
            );
            """
        await self.execute(sql.format(*(self._table for _ in range(2))))

        # Add Trigger
        sql = """
            CREATE TRIGGER IF NOT EXISTS "{}.trigger.after.insert"
            AFTER INSERT ON "{}"
            BEGIN
                UPDATE "{}"
                SET
                    "season" = json_extract("data", '$.parameters.Season'),
                    "season_type" = json_extract("data", '$.parameters.SeasonType'),
                    "total_player_games" = (
                        SELECT count(*) FROM json_each(json_extract("data", '$.resultSets[0].rowSet'))
                    )
                WHERE rowid = NEW.rowid;
            END;
            """
        await self.execute(sql.format(*(self._table for _ in range(3))))

    async def delete(self, season, season_type=SeasonTypeAllStar.default) -> None:
        sql = 'DELETE FROM "{}" WHERE season  = ? AND season_type = ?;'
        await self.execute(sql.format(self._table), (season, season_type))
