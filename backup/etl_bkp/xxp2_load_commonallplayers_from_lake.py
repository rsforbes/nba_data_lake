import asyncio
import nba_data_lake.lake.nba.commonallplayers as lake


async def main() -> None:
    await lake.CommonAllPlayers().extract("nba.commonallplayers")


if __name__ == "__main__":
    asyncio.run(main())
