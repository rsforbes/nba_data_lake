import asyncio
import nba_data_lake.apis.nba_stats as api
import nba_data_lake.lake.nba.commonallplayers as lake


async def main() -> None:
    await lake.CommonAllPlayers().create()

    await lake.CommonAllPlayers().delete()

    data = api.CommonAllPlayers.get()

    await lake.CommonAllPlayers().insert(data)


if __name__ == "__main__":
    asyncio.run(main())
