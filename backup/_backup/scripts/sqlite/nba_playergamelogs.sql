--SELECT SCRIPTS
-- Total NBA Games per Season
SELECT season_id, count(DISTINCT(game_id)) 
FROM nba_playergamelogs
GROUP BY season_id


-- Days Rest for LeBron James (Given a Specific Season)
SELECT player_id, game_date, days_rest
FROM nba_playergamelogs
WHERE player_id = 2544
AND season_id = 22021
ORDER BY game_date ASC

-- UPDATE SCRIPTS
-- Populate Days reset for every Season, Player, and Game
UPDATE nba_playergamelogs AS cur
SET days_rest = (julianday(cur.game_date)- julianday((SELECT MAX(prev.game_date) FROM nba_playergamelogs AS prev
                                                      WHERE prev.player_id = cur.player_id 
													  AND prev.game_date < cur.game_date
													  AND prev.season_id = cur.season_id)) - 1)
WHERE EXISTS (SELECT 1 FROM nba_playergamelogs AS prev
              WHERE prev.player_id = cur.player_id
                AND prev.game_date < cur.game_date
				and prev.season_id = cur.season_id)