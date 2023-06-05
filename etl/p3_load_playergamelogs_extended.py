import asyncio
import nba_data_lake.db.nba.playergamelogs_extended as pgl_extended
import nba_data_lake.db.nba.seasons as nba_seasons


async def main() -> None:
    # Create
    await pgl_extended.PlayerGameLogsExtended().create()

    # Load
    params = await nba_seasons.Seasons().select()
    for param in params:
        season_id = param["season_id"]
        print(f"SeasonID: {season_id}.")
        await pgl_extended.PlayerGameLogsExtended().load(season_id)


if __name__ == "__main__":
    asyncio.run(main())
