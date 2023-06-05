import asyncio
import nba_data_lake.apis.nba_stats as api
import nba_data_lake.lake.nba.lakeplayerindex as lake


async def get() -> str:
    return api.PlayerIndex().get()


async def delete():
    await lake.PlayerIndex().delete()


async def insert(data: str):
    await lake.PlayerIndex().insert(data)


async def main() -> None:
    await delete()
    await insert(await get())


if __name__ == "__main__":
    asyncio.run(main())
