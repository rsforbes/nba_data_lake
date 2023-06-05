import asyncio
import nba_data_lake.db.nba.playergamelogs_players as db
import nba_data_lake.db.nba.seasons as nba_seasons


async def main() -> None:
    # Drop
    # await db.PlayerGameLogsPlayers().drop()

    # Create
    await db.PlayerGameLogsPlayers().create()

    # Load
    params = await nba_seasons.Seasons().select()
    for param in params:
        season_id = param["season_id"]
        print(f"Season: {season_id}")
        await db.PlayerGameLogsPlayers().load(season_id)


if __name__ == "__main__":
    asyncio.run(main())
