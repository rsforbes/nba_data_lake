UPDATE pro_sports_transactions
SET player_id = json_extract(VALUE, '$[0]') AS player_id,
FROM 
	commonallplayers, 
	json_each(raw -> '$.resultSets[0].rowSet') p
JOIN
	pro_sports_transactions ps
ON 
	json_extract(VALUE, '$[2]') = ps.acquired
WHERE 
-- 	json_extract(json_each.value, '$[2]') = 'LeBron James' AND
	ps.transaction_type = 'INJURY' AND
	yr_inj BETWEEN yr_1 AND yr_n
LIMIT 20