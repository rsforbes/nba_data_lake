SELECT 
  	json_extract(PARAMETERS, '$.Season') AS season,
  	json_extract(VALUE, '$[0]') AS season_id,
  	json_extract(PARAMETERS, '$.SeasonType') AS season_type,
  	json_extract(VALUE, '$[6]') AS game_id,
  	json_extract(VALUE, '$[7]') AS game_date,
	json_extract(VALUE, '$[1]') AS player_id,
  	json_extract(VALUE, '$[10]') AS minutes,
  	VALUE AS json
FROM 
  leaguegamelog_player, 
  json_each(raw -> '$.resultSets[0].rowSet')
WHERE 
	json_extract(PARAMETERS, '$.Season') IN ('2022-23', '2021-22', '2020-21')
	AND season_type IN ('Regular Season')