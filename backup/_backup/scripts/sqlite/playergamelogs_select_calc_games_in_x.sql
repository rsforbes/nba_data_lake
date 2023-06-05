SELECT cur.player_id, cur.game_date, days_rest,
(
	SELECT count(*)
	FROM nba_playergamelogs prev
	WHERE prev.game_date > date(cur.game_date,'-3 days')
	AND prev.game_date <= cur.game_date
	AND prev.player_id = cur.player_id
) AS games_in_3d,
(
	SELECT count(*)
	FROM nba_playergamelogs prev
	WHERE prev.game_date > date(cur.game_date,'-7 days')
	AND prev.game_date <= cur.game_date
	AND prev.player_id = cur.player_id
) AS games_in_7d,
(
	SELECT count(*)
	FROM nba_playergamelogs prev
	WHERE prev.game_date > date(cur.game_date,'-14 days')
	AND prev.game_date <= cur.game_date
	AND prev.player_id = cur.player_id
) AS games_in_14d,
(
	SELECT count(*)
	FROM nba_playergamelogs prev
	WHERE prev.game_date > date(cur.game_date,'-30 days')
	AND prev.game_date <= cur.game_date
	AND prev.player_id = cur.player_id
	AND prev.season_id = cur.season_id
) AS games_in_30d
FROM nba_playergamelogs cur
LIMIT 300





