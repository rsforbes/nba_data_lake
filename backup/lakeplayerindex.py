from ..src.nba_data_lake.lake.nba.nbabasedb import NBABaseDB


class PlayerIndex(NBABaseDB):
    def __init__(self) -> None:
        super().__init__("lake.nba.stats.api.playerindex")

    async def extract(self) -> None:
        sql = """
            SELECT json_extract(value, '$[0]') as person_id,
                json_extract(value, '$[1]') as player_last_name,
                json_extract(value, '$[2]') as player_first_name,
                json_extract(value, '$[3]') as player_slug,
                json_extract(value, '$[4]') as team_id,
                json_extract(value, '$[5]') as team_slug,
                json_extract(value, '$[6]') as is_defunct,
                json_extract(value, '$[7]') as team_city,
                json_extract(value, '$[8]') as team_name,
                json_extract(value, '$[9]') as team_abbreviation,
                json_extract(value, '$[10]') as jersey_number,
                json_extract(value, '$[11]') as position,
                json_extract(value, '$[12]') as height,
                json_extract(value, '$[13]') as weight,
                json_extract(value, '$[14]') as college,
                json_extract(value, '$[15]') as country,
                json_extract(value, '$[16]') as draft_year,
                json_extract(value, '$[17]') as draft_round,
                json_extract(value, '$[18]') as draft_number,
                json_extract(value, '$[19]') as roster_status,
                json_extract(value, '$[20]') as pts,
                json_extract(value, '$[21]') as rbs,
                json_extract(value, '$[22]') as ast,
                json_extract(value, '$[23]') as stats_timeframe,
                json_extract(value, '$[24]') as from_year,
                json_extract(value, '$[25]') as to_year
            FROM "{}",
                json_each(data, '$.resultSets[0].rowSet');
            """
        return await self.fetchall(sql.format(self._table))
