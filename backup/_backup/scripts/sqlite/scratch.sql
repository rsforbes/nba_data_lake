SELECT *
FROM "lake.prosports.transactions" t
JOIN "etl.prosports.transactions.players" p
ON 
	t.relinquished = p.player_name
	AND p.nba_player_id IS NOT NULL
WHERE "date" > '2022-01-01'
AND t.transaction_type in ('il','injury')





SELECT 
t.id as pro_sports_transaction_id, t.date as pro_sports_transaction_date, t.team as pro_sports_transaction_team, 
t.relinquished as pro_sports_transaction_player_name, t.notes as pro_sports_transaction_notes,
p.id as pro_sports_transaction_player_id, p.nba_player_id, p.nba_player_name,
pgl.game_id as nba_game_id, pgl.game_date as nba_game_date, pgl.nba_player_days_rest
FROM "lake.prosports.transactions" t
JOIN "etl.prosports.transactions.players" p
	ON t.relinquished = p.player_name
	AND p.nba_player_id IS NOT NULL
JOIN "stats.nba.playergamelogs.daysrest" pgl
	ON p.nba_player_id = pgl.player_id
	AND pgl.game_id = 
		(SELECT dr.game_id 
		FROM "stats.nba.playergamelogs.daysrest" dr
		WHERE p.nba_player_id = pgl.player_id
		AND dr.game_date <= t."date"
		LIMIT 1)
WHERE t."date" > '2022-01-01'
AND t.transaction_type in ('il','injury')



CASE
	WHEN instr(player_name,'.') THEN replace(player_name,'.','')
END as player_name_1,
trim(substr(player_name, 0, instr(player_name,'/'))) as player_name_2, 
trim(substr(player_name, instr(player_name,'/'),  length(player_name) - instr(player_name,'/'))) as player_name_3, 