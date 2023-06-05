import aiosqlite
from itertools import dropwhile
import json
import logging


class Sqlite:
    def __init__(self, database_path: str):
        self.db_path = database_path

    # Delete Table Methods
    async def drop_table_transactions(self) -> None:
        logging.info("Dropping table transactions")
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DROP TABLE IF EXISTS transactions;")
            await db.commit()

    # Create Table Methods
    async def create_table_transactions(self) -> None:
        logging.info("Creating table transactions")
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS transactions (\
                    transaction TEXT NO CASE,\
                    date_created TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))),\
                    date_modified TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))));"
            )
            await db.commit()

    # Data Management Methods
    async def post_transaction(self, transaction: str) -> None:
        try:
            acquired = transaction["Acquired"]
            if acquired is not None:
                acquired = acquired.split("\u2022")
                acquired = list(map(lambda x: x.strip(), acquired))
                acquired = list(dropwhile(lambda x: x == "", acquired))
                if len(acquired) == 0:
                    acquired = None
                elif len(acquired) == 1:
                    acquired = acquired[0]
                else:
                    acquired = json.dumps(acquired)

            relinquished = transaction["Relinquished"]
            if relinquished is not None:
                relinquished = relinquished.split("\u2022")
                relinquished = list(map(lambda x: x.strip(), relinquished))
                relinquished = list(dropwhile(lambda x: x == "", relinquished))
                if len(relinquished) == 0:
                    relinquished = None
                elif len(relinquished) == 1:
                    relinquished = relinquished[0]
                else:
                    relinquished = json.dumps(relinquished)

            notes = transaction["Notes"]
            if notes is not None:
                notes = notes.split("\u2022")
                notes = list(map(lambda x: x.strip(), notes))
                notes = list(dropwhile(lambda x: x == "", notes))
                if len(notes) == 0:
                    notes = None
                elif len(notes) == 1:
                    notes = notes[0]
                else:
                    notes = json.dumps(notes)

            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT INTO pro_sports_transactions (date, team, acquired,\
                        relinquished, notes, transaction_type) values (?,?,?,?,?,?)",
                    (transaction),
                )
                await db.commit()
        except Exception:
            logging.exception()

            # Attempt to record the data at the point the exception occurred
            try:
                logging.error(f"Exception data: {transaction}")
            except Exception:
                logging.exception()
