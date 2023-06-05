import asyncio
import nba_data_lake.apis.nba_stats as api
import nba_data_lake.lake.nba.playergamelogs as lake
import nba_data_lake.db.nba.seasons as nba_seasons
from time import sleep


async def get(season, season_type) -> str:
    return api.PlayerGameLogs().get(season, season_type)


async def insert(data: str):
    await lake.PlayerGameLogs().insert((data,))


async def main() -> None:
    await lake.PlayerGameLogs().create()
    params = await nba_seasons.Seasons().select()
    for param in params:
        season = param["season"]
        season_type = param["segment"]
        print(f"Season: {season}, Season_Type: {season_type}")
        await insert(await get(season, season_type))
        sleep(0.600)


if __name__ == "__main__":
    asyncio.run(main())
