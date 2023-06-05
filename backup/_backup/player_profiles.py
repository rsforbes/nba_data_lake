from pathlib import Path
import asyncio
import time
import json
import api.nba.nbalib as nbalib
from nba_api.stats.library.parameters import SeasonTypeAllStar

nba_sqlite = Path.cwd().joinpath("nba.sqlite")


async def main() -> None:
    player_ids_exclusion = await nbalib.get_playercareerstats()

    # Get NBA Common All Players
    await nbalib.drop_table("commonallplayers")
    await nbalib.create_table("commonallplayers")
    data = await nbalib.get_commonallplayers()
    players = json.loads(data.result_sets)[0]["rowSet"]

    # GET NBA Player Profiles
    # await nbalib.drop_table("playercareerstats")
    await nbalib.create_table("playercareerstats")
    for player in players:
        if player[0] in player_ids_exclusion:
            continue

        await nbalib.get_playerprofilev2(player_id=player[0])
        print(player[2])
        time.sleep(0.600)


if __name__ == "__main__":
    asyncio.run(main())
