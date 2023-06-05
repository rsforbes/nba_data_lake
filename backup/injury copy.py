import aiosqlite
import asyncio
from datetime import date
from itertools import dropwhile
import json
import logging
import pro_sports_transactions as pst
from typing import overload, List, Dict


class Injury:
    def __init__(self, database_path: str):
        self.db_path = database_path

    # Delete Table Methods
    async def drop_pro_sports_transactions_table(self) -> None:
        logging.info('Dropping table "prosports.transactions"')
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('DROP TABLE IF EXISTS "prosports.transactions";')
            await db.commit()

    # Create Table Methods
    async def create_pro_sports_transactions_table(self) -> None:
        logging.info('Creating table "prosports.transactions"')
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS \"prosports.transactions\" (\
                    transaction_id INTEGER PRIMARY KEY,\
                    date TEXT NO CASE,\
                    team TEXT NO CASE,\
                    acquired TEXT NO CASE,\
                    relinquished TEXT NO CASE,\
                    notes TEXT NO CASE,\
                    transaction_type TEXT NO CASE,\
                    date_created TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))),\
                    date_modified TEXT DEFAULT (strftime(\
                        '%Y-%m-%dT%H:%M:%f+00:00', datetime('now'))));"
            )
            await db.commit()

    # Data Management Methods
    async def post_transaction(
        self, data: Dict, league: str, transaction_type: str
    ) -> None:
        print(f"Posting data for {transaction_type}")

        acquired = data["Acquired"]
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

        relinquished = data["Relinquished"]
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

        notes = data["Notes"]
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

        s_date = data["Date"]
        team = data["Team"]

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'INSERT or IGNORE INTO "prosports.transactions" (date, team, acquired,\
                    relinquished, notes, league, transaction_type) VALUES (?,?,?,?,?,?,?)',
                (
                    s_date,
                    team,
                    "" if acquired is None else acquired,
                    "" if relinquished is None else relinquished,
                    "" if notes is None else notes,
                    league,
                    transaction_type,
                ),
            )
            await db.commit()

    @overload
    async def populate_pro_sports_transactions_table(
        self, begin_date: date, transaction_types: List[pst.TransactionType]
    ) -> None:
        """Creates an asyncio task for each TransactionType to retrieve data from \
        prosportstransactions.com and populate the database"""

        try:
            # Create three worker tasks to process the queue concurrently.
            tasks = []
            for transaction_type in transaction_types:
                logging.info(f"Launching Transaction Type {transaction_type}")
                task = asyncio.create_task(
                    self.populate_pro_sports_transactions_table(
                        begin_date, transaction_type
                    )
                )
                tasks.append(task)

            # Wait until all worker tasks are cancelled.
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            logging.exception()

    @overload
    async def populate_pro_sports_transactions_table(
        self, begin_date: date, transaction_type: pst.TransactionType
    ) -> None:
        """Retrieves data from prosportstransactions.com and populates the database

        Parameters
        ----------
        begin_date : date, required
            The date in which to start pulling all transactions

        transaction_type: TransactionType, required
            The Transaction Type for which to pull data
        """
        # Get NBA Pro Sports Transactions
        page_number = 0
        while True:
            injuries = await pst.Search(
                begin_date=begin_date.isoformat(),
                page_number=page_number,
                transaction_types=(transaction_type,),
            ).get_dict()

            if injuries is None or len(injuries["records"]) == 0:
                return

            for injury in injuries["records"]:
                await self.post_transaction(injury, transaction_type)

            page_number += 25

    async def get_max_transaction_date(self, transaction_type: str) -> None:
        """Retrieves the max transaction date from the \"prosports.transactions\" table."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                'SELECT max(date) FROM "prosports.transactions" WHERE transaction_type = ?',
                [transaction_type],
            ) as cursor:
                row = await cursor.fetchone()
                return None if row is None or len(row) == 0 else row[0]
