INSERT INTO nba_playergamelogs_extended
SELECT cur.season_id, cur.player_id, cur.player_name, cur.team_id, cur.team_name, cur.game_id, cur.game_date,
	(
		julianday(cur.game_date)	
		- julianday((SELECT MAX(prev.game_date) 
					FROM nba_playergamelogs AS prev
					JOIN nba_seasons s
					ON prev.game_date BETWEEN s.start AND s.end
					WHERE prev.player_id = cur.player_id 
					AND prev.game_date < cur.game_date
					AND prev.season_id = cur.season_id
					AND cur.game_date BETWEEN s.start AND s.end)) 
		- 1) AS days_rest						  
FROM nba_playergamelogs AS cur	
JOIN nba_seasons s
ON cur.game_date BETWEEN s.start AND s.end	
AND s.segment = 'Regular Season'
AND s.season = '2022-23'			  
WHERE EXISTS (SELECT 1 FROM nba_playergamelogs AS prev
				JOIN nba_seasons s
				ON prev.game_date BETWEEN s.start AND s.end			  
				WHERE prev.player_id = cur.player_id
				AND prev.game_date < cur.game_date
				and prev.season_id = cur.season_id
				AND cur.game_date BETWEEN s.start AND s.end)
order by game_date desc



select p.player_name, n.display_first_last, p.from_year, n.from_year, p.to_year, n.to_year, n.player_id
from pro_sports_players p
join nba_players n
on instr(p.player_name, n.display_first_last)
and p.from_year >= n.from_year 
and p.to_year <= n.to_year +1
and p.nba_player_id is null
and p.from_year > 1999
and p.player_name NOT LIKE '%(b.%'
order by p.from_year

