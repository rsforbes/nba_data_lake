from .nbabasedb import NBABaseDB


class CommonAllPlayers(NBABaseDB):
    def __init__(self) -> None:
        super().__init__("lake.nba.stats.api.commonallplayers")

    async def extract(self, target) -> None:
        sql = """
            INSERT OR REPLACE INTO "{}"
            SELECT
                json_extract(value, '$[0]') as person_id,
                json_extract(value, '$[1]') as display_last_common_first,
                json_extract(value, '$[2]') as display_first_last,
                json_extract(value, '$[3]') as rosterstatus,
                json_extract(value, '$[4]') as from_year,
                json_extract(value, '$[5]') as to_year,
                json_extract(value, '$[6]') as player_code,
                json_extract(value, '$[7]') as player_slug,
                json_extract(value, '$[8]') as team_id,
                json_extract(value, '$[9]') as team_city,
                json_extract(value, '$[10]') as team_name,
                json_extract(value, '$[11]') as team_abbreviation,
                json_extract(value, '$[12]') as team_code,
                json_extract(value, '$[13]') as team_slug,
                json_extract(value, '$[14]') as games_played_flag,
                json_extract(value, '$[15]') as otherleague_experience_ch
            FROM
                "{}",
                json_each(data, '$.resultSets[0].rowSet');
            """
        await self.execute(sql.format(target, self._table))
