-- Groups Pro Sports Transaction Players
-- Selects the first year that an Injury / IL occurred: min()
-- Selects the last year that an Injury / IL occurred: max()
-- The data is inserted into Pro Sports Players
-- The data can then be matched up with player_ids from nba_players
insert into pro_sports_players (player_name, from_year, to_year)
select player_name, min(from_year) as from_year, max(to_year) as to_year from
(select acquired as player_name, min(strftime('%Y',date)) as from_year, max(strftime('%Y',date)) as to_year
from pro_sports_transactions_raw
where transaction_type in ('il','injury')
and acquired not in ('','11/25/2019')
and acquired not like 'placed%'
group by acquired
UNION
select relinquished as player_name, min(strftime('%Y',date)) as from_year, max(strftime('%Y',date)) as to_year
from pro_sports_transactions_raw
where transaction_type in ('il','injury')
and relinquished not in ('','11/25/2019', 'v')
and relinquished not like 'placed%'
and relinquished not like '%(DTD)%'
group by relinquished)
group by player_name

=======BETTER=======
--includes team
-- will need to pull down additional data; perhaps career stats to determine team.
-- needs to be combined with pro_sports_players_update_from_nba_players.sql for a single action update!!!!
select player_name, team, min(from_year) as from_year, max(to_year) as to_year from
(select acquired as player_name, team, min(strftime('%Y',date)) as from_year, max(strftime('%Y',date)) as to_year
from "lake.prosports.transactions"
where transaction_type in ('il','injury')
and acquired not in ('','11/25/2019')
and acquired not like 'placed%'
group by acquired, team
UNION
select relinquished as player_name, team, min(strftime('%Y',date)) as from_year, max(strftime('%Y',date)) as to_year
from "lake.prosports.transactions"
where transaction_type in ('il','injury')
and relinquished not in ('','11/25/2019', 'v')
and relinquished not like 'placed%'
and relinquished not like '%(DTD)%'
group by relinquished, team)
group by player_name, team


