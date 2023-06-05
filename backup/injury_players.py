from ..src.nba_data_lake.db.pro_sports_transactions.base import Base
import aiosqlite
import json


class InjuryPlayers(Base):
    _table = "prosports.transactions.injuries.players"

    def __init__(self) -> None:
        Base.__init__(self)

    async def drop(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(f'DROP TABLE IF EXISTS "{self._table}"')
            await db.commit()

    async def create(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            sql = f"""
                CREATE TABLE IF NOT EXISTS "{self._table}" (
                    "player_id"	INTEGER,
                    "player_name" TEXT NO CASE,
                    "player_names" TEXT NO CASE,
                    "team_name"	TEXT NO CASE,
                    "from_year"	TEXT NO CASE,
                    "to_year"	TEXT NO CASE,
                    PRIMARY KEY("player_id" AUTOINCREMENT),
                    CONSTRAINT "uq_prosports_transactions_players" UNIQUE("player_name","team_name")
                )
               """
            await db.execute(sql)
            await db.commit()

    async def insert(self, players: str) -> None:
        async with aiosqlite.connect(self._path) as db:
            sql = f"""
                INSERT OR IGNORE INTO "{self._table}"
                (player_name, player_names, team_name, min_injury_date, max_injury_date) VALUES (?,?,?,?,?)
                """
            for player in players:
                await db.execute(
                    sql,
                    (
                        player["player_name"],
                        json.dumps(player["player_names"]),
                        player["team_name"],
                        player["from_year"],
                        player["to_year"],
                    ),
                )
            await db.commit()
