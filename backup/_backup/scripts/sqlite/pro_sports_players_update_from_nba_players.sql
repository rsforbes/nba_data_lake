-- Adds additional nba_player data to the 
-- pro_sports_players data based on nba_player_id
UPDATE "etl.prosports.transactions.players" AS p
SET 
	nba_player_id = n.player_id,
	nba_from_year = n.from_year,
	nba_to_year = n.to_year,
	nba_display_first_last = n.display_first_last
FROM "etl.nba.commonallplayers" n
WHERE
	(
		instr(replace(p.player_name,'.',''), n.display_first_last) 
		OR instr(n.display_first_last, replace(p.player_name,'.',''))
	)	
	AND p.from_year >= n.from_year - 2  -- found to be reasonable based on data
	AND p.to_year <= n.to_year + 2 -- found to be reasonable based on data
	AND p.nba_player_id is null
	AND p.player_name NOT LIKE '%(b.%'
