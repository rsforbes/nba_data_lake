import aiosqlite


class Player:
    def __init__(self, sqlite_path: str):
        self._db = sqlite_path

    async def insert(self, data: dict):
        # Define the SQL statement to replace a row in the table
        sql = """
        REPLACE INTO nba_players (
            person_id, first_name, last_name, display_first_last, display_last_comma_first, display_fi_last,
            player_slug, birthdate, school, country, last_affiliation, height, weight, season_exp, jersey,
            position, rosterstatus, games_played_current_season_flag, team_id, team_name, team_abbreviation,
            team_code, team_city, playercode, from_year, to_year, dleague_flag, nba_flag, games_played_flag,
            draft_year, draft_round, draft_number, greatest_75_flag
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        """

        # Connect to the database
        async with aiosqlite.connect(self._db) as conn:
            # Execute SQL
            await conn.execute(sql, data)

            # Commit the changes and close the database connection
            await conn.commit()
