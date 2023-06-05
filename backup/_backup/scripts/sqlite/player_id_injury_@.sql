SELECT
	ps.transaction_id,
 	json_extract(VALUE, '$[0]') AS player_id,
 	json_extract(VALUE, '$[2]') AS player,
 	json_extract(VALUE, '$[4]') AS yr_1,
 	json_extract(VALUE, '$[5]') AS yr_n,
	substring(ps."date",1,4) AS yr_inj,
 	value
FROM 
	commonallplayers, 
	json_each(raw -> '$.resultSets[0].rowSet') p
JOIN
	pro_sports_transactions ps
ON 
	json_extract(VALUE, '$[2]') = ps.acquired
	AND CAST(substring(ps.date,1,4) AS INTEGER) 
		BETWEEN CAST(json_extract(p.VALUE, '$[4]') AS INTEGER) 
		AND CAST(json_extract(p.VALUE, '$[5]') AS INTEGER)
 WHERE 
	ps.transaction_type = 'INJURY'
 	AND CAST(substring(ps.date,1,4) AS INTEGER) BETWEEN 2021 AND 2023
-- GROUP BY transaction_id, player_id
-- HAVING COUNT > 1
LIMIT 10


-- 	json_extract(json_each.value, '$[2]') = 'LeBron James'
	
