from ...library.basedb import BaseDB


class TransactionsInjuriesExtended(BaseDB):
    def __init__(self) -> None:
        BaseDB.__init__(self, "prosports.transactions.injuries.extended")
        self._init_sql()

    async def drop(self) -> None:
        sql = 'DROP TABLE IF EXISTS "{}"'
        await self.execute(sql.format(self._table))

    async def create(self) -> None:
        # Create Table
        await self.execute(self._sql.table.format(self._table))
        await self.create_indexes()
        await self.create_triggers()

    async def create_indexes(self):
        params = (self._table,) * 2
        for index in self._sql.indexes:
            await self.execute(index.format(*params))

    async def create_triggers(self):
        params = (self._table,) * 3

        await self.execute(
            self._sql.triggers[0].format(*params, "nba.playergamelogs.players")
        )

    async def get_max_transaction_dates(self) -> None:
        sql = """
            SELECT
                MAX(transaction_date) as max_transaction_date,
                transaction_type
            FROM "{}"
            GROUP BY transaction_type
            """
        return await self.fetchall(sql.format(self._table))

    async def load(
        self,
        start_date: str,
        transaction_type: str,
        source_table: str = "lake.prosports.transactions",
    ) -> None:
        sql = """
            INSERT OR IGNORE INTO "{}"
            (transaction_id, transaction_date, team_name, acquired, relinquished, notes, transaction_type)
            SELECT
                transaction_id,
                json_extract(transaction_data,'$.Date') AS "transaction_date",
                json_extract(transaction_data,'$.Team') AS team_name,
                trim(replace(json_extract(transaction_data,'$.Acquired'),'•','')) AS acquired,
                trim(replace(json_extract(transaction_data,'$.Relinquished'),'•','')) AS relinquished,
                json_extract(transaction_data,'$.Notes') as notes,
                transaction_type
            FROM "{}"
            WHERE transaction_type = ?
            AND relinquished != ''
            AND transaction_date > ?
            """
        await self.execute(
            sql.format(self._table, source_table), (transaction_type, start_date)
        )

    def _init_sql(self):
        self._sql = self._SQL(
            indexes=[
                """
                CREATE INDEX IF NOT EXISTS "idx.{}.relinquished"
                ON "{}" (
                    "relinquished",
                    "team_name",
                    "transaction_date",
                    "notes"
                ) WHERE relinquished != '';
            """,
            """
            CREATE INDEX "idx.prosports.transactions.injuries.extended.nba_season_id"
            ON "prosports.transactions.injuries.extended" ("nba_season_id")
            """
            ],
            insert="",
            load="""
            INSERT OR IGNORE INTO "{}"
            SELECT
                transaction_id,
                json_extract(transaction_data,'$.Date') AS "transaction_date",
                json_extract(transaction_data,'$.Team') AS team_name,
                trim(replace(json_extract(transaction_data,'$.Acquired'),'•','')) AS acquired,
                trim(replace(json_extract(transaction_data,'$.Relinquished'),'•','')) AS relinquished,
                json_extract(transaction_data,'$.Notes') as notes,
                transaction_type
            FROM "{}"
                """,
            table="""
                CREATE TABLE IF NOT EXISTS "{}" (
                    "transaction_id" INTEGER,
                    "transaction_date" TEXT,
                    "team_name"	TEXT,
                    "acquired" TEXT,
                    "relinquished" TEXT,
                    "notes"	TEXT,
                    "transaction_type"	TEXT,
                    "nba_player_id" INTEGER,
                    "nba_team_id" INTEGER,
                    "nba_season_id"	TEXT,
                    "nba_season" TEXT,
                    "nba_season_type" TEXT,
                    "nba_team_name"	TEXT,
                    "nba_season_start" TEXT,
                    "nba_season_end" TEXT,
                    "nba_player_start" TEXT,
                    "nba_player_end" TEXT,
                    PRIMARY KEY("transaction_id"));
            """,
            triggers=[
                """
                CREATE TRIGGER IF NOT EXISTS "{}.trigger.after.insert"
                AFTER INSERT ON "{}"
                BEGIN
                UPDATE "{}"
                SET
                    nba_player_id = NBA.player_id,
                    nba_team_id = NBA.team_id,
                    nba_season_id = NBA.season_id,
                    nba_season = NBA.season,
                    nba_season_type = NBA.season_type,
                    nba_team_name = NBA.team_name,
                    nba_season_start = NBA.season_start_date,
                    nba_season_end = NBA.season_end_date,
                    nba_player_start = NBA.player_start_date,
                    nba_player_end = NBA.player_end_date
                FROM "{}" NBA
                WHERE
                    instr(NEW.relinquished, NBA.player_name)
                    AND instr(NBA.team_name, NEW.team_name)
                    AND NEW.transaction_date BETWEEN NBA.season_start_date AND NBA.season_end_date
                    AND transaction_id = NEW.transaction_id;
                END;
                """
            ],
            select="",
        )
