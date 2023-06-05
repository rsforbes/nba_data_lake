from ..src.nba_data_lake.db.basedb import BaseDB
import aiosqlite


class PlayersNBA(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(self)

    async def create(self) -> None:
        sql = """
                CREATE TABLE IF NOT EXISTS "prosports.transactions.players.nba" (
                "player_id"	INTEGER,
                "player_name" TEXT,
                "player_names"	TEXT,
                "team_name"	TEXT,
                "from_year"	TEXT,
                "to_year"	TEXT,
                "nba_season_id"	INTEGER,
                "nba_player_id"	INTEGER,
                "nba_player_name" TEXT,
                "nba_team_id" INTEGER,
                "nba_team_name"	TEXT,
                "nba_from_year"	TEXT,
                "nba_to_year" TEXT,
                CONSTRAINT "uq_prosports_transactions_players_players" UNIQUE("player_name","team_name"))
                """

        async with aiosqlite.connect(self._path) as db:
            await db.execute(sql)
            await db.commit()

    async def load(self) -> None:
        sql = """
            INSERT INTO "prosports.transactions.players.nba"
            SELECT *
            FROM "prosports.transactions.players" p
            JOIN "nba.playergamelogs.players" n
            ON instr(p.player_names, n.player_name)
            AND instr(n.team_name, p.team_name)
            AND n.from_year <= p.from_year
            AND n.to_year + 1 >= p.to_year
            where p.player_id not in
            (select player_id from "prosports.transactions.players.nba")
            order by player_id
            """

        async with aiosqlite.connect(self._path) as db:
            await db.execute(sql)
            await db.commit()
