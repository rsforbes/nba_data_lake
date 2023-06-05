from pathlib import Path
import asyncio
import time
import json
import web.nbalib as nbalib
from nba_api.stats.library.parameters import SeasonTypeAllStar


async def main() -> None:
    # player_ids_exclusion = await nbalib.get_playercareerstats()

    # # Get NBA Common All Players
    # await nbalib.drop_table("commonallplayers")
    # await nbalib.create_table("commonallplayers")
    # data = await nbalib.get_commonallplayers()
    # players = json.loads(data.result_sets)[0]["rowSet"]

    # # GET NBA Player Profiles
    # # await nbalib.drop_table("playercareerstats")
    # await nbalib.create_table("playercareerstats")
    # for player in players:

    #     if player[0] in player_ids_exclusion:
    #         continue

    #     await nbalib.get_playerprofilev2(player_id=player[0])
    #     print(player[2])
    #     time.sleep(.600)

    # GET NBA Game logs
    # await nbalib.drop_table("leaguegamelog")
    await nbalib.create_table("leaguegamelog")

    # start = 1946
    start = 1946
    end = 2019
    seasons = [f"{r}-{str(r+1)[2:]}" for r in range(start, end)]
    # season_types = [SeasonTypeAllStar.regular, SeasonTypeAllStar.all_star, SeasonTypeAllStar.preseason, "PlayIn"]

    season_types = [
        SeasonTypeAllStar.regular,
        SeasonTypeAllStar.all_star,
        SeasonTypeAllStar.preseason,
    ]
    for season in seasons:
        for season_type in season_types:
            await nbalib.get_leaguegamelogs(season=season, season_type=season_type)
            print(season)
            time.sleep(0.600)


if __name__ == "__main__":
    asyncio.run(main())
