INSERT INTO prosportstransactions
	SELECT 
	json_object(
	"name","prosportstransactions",
	"headers", json_array("date", "team", "acquired", "relinquished", "notes", "transaction_type"),
	"rowSet", json_group_array(json(js))) AS json from 
	(SELECT json_array("date", team, acquired, relinquished, notes, "transaction_type") AS js FROM pro_sports_transactions_copy
	ORDER BY "date")
