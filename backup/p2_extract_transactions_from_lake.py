import asyncio
import nba_data_lake.db.pro_sports_transactions.transactions as db


async def main() -> None:
    # Drop
    # await db.Transactions().drop()

    # Create
    await db.Transactions().create()

    # Load
    await db.Transactions().load()


if __name__ == "__main__":
    asyncio.run(main())
