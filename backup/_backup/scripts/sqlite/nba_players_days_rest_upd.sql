-- Original
UPDATE nba_playergamelogs AS cur
SET days_rest = (
    julianday(cur.game_date)- julianday((SELECT MAX(prev.game_date) FROM nba_playergamelogs AS prev
                                                      WHERE prev.player_id = cur.player_id 
													  AND prev.game_date < cur.game_date
													  AND prev.season_id = cur.season_id)) - 1)
WHERE EXISTS (SELECT 1 FROM nba_playergamelogs AS prev
              WHERE prev.player_id = cur.player_id
                AND prev.game_date < cur.game_date
				and prev.season_id = cur.season_id)


-- Option 2
UPDATE nba_playergamelogs AS curr
SET days_rest = (
    SELECT
        julianday(cur.game_date) - julianday(MAX(prev.game_date)) - 1
    FROM
        nba_playergamelogs AS prev
    JOIN nba_seasons AS s ON prev.game_date BETWEEN s.start AND s.end
    WHERE
        prev.player_id = curr.player_id
        AND prev.game_date < curr.game_date
        AND prev.season_id = curr.season_id
        AND curr.game_date BETWEEN s.start AND s.end
        AND s.season = '2022-23'
		AND s.segment = 'Regular Season'
    )
WHERE
    EXISTS (
        SELECT 1
        FROM
            nba_playergamelogs AS prev
        JOIN nba_seasons AS s ON prev.game_date BETWEEN s.start AND s.end
        WHERE
            prev.player_id = curr.player_id
            AND prev.game_date < curr.game_date
            AND prev.season_id = curr.season_id
            AND curr.game_date BETWEEN s.start AND s.end
            AND s.season = '2022-23'
			AND s.segment = 'Regular Season'
    );
