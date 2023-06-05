import asyncio
import nba_data_lake.db.nba.playergamelogs as db
import nba_data_lake.db.nba.seasons as nba_seasons


async def main() -> None:
    # Create
    await db.PlayerGameLogs().create_table()

    # Load
    params = await nba_seasons.Seasons().select()
    for param in params:
        season = param["season"]
        season_type = param["segment"]
        print(f"Season: {season}, SeasonType: {season_type}")
        await db.PlayerGameLogs().load(season, season_type)


if __name__ == "__main__":
    asyncio.run(main())
