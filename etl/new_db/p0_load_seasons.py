import asyncio
import nba_data_lake.db.nba.seasons as db


async def main() -> None:
    await db.Seasons().create_table()
    await db.Seasons().create_indexes()
    await db.Seasons().load()


if __name__ == "__main__":
    asyncio.run(main())
