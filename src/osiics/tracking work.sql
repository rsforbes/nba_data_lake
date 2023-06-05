-- REQUIRED TO MATCH ON " ARM "
update "osiics.v14.ioc"
set region = 'Arm'
where region = 'Upper Arm'

-- Update Data
REIMPORT osiics.v14.ioc DATA DUE TO SPACES THAT CANNOT BE TRIMMED. :-(

-- Identify Region
UPDATE "prosports.transactions.injuries.extended" AS t
SET osiics_region = a.osiics_region
FROM
(
	SELECT 
		regexp_substr(
		i.notes,
		(
			SELECT '\b(' || group_concat(region,'|') || ')\b' AS regions
			FROM
			(
				SELECT lower(replace(region,"/","|")) as region
				FROM "osiics.v14.ioc"
				GROUP BY region
			)	
		)) as osiics_region, i.transaction_id
	FROM "prosports.transactions.injuries.extended" i
--	WHERE i.transaction_date > '2020-01-01'
	WHERE regexp_like(lower(notes), 
		(
			SELECT '\b(' || group_concat(region,'|') || ')\b' AS regions
			FROM
			(
				SELECT lower(replace(region,"/","|")) as region
				FROM "osiics.v14.ioc"
				GROUP BY region
			)	
	))
) a
WHERE t.transaction_id = a.transaction_id 


-- Injuries
select 
lower(trim(regexp_replace(
	trim(regexp_replace(inj.notes, (
	SELECT 
	'\b' || group_concat(keyword,'\b|\b') || '\b|\(.*\)'  as exclusion
	FROM "prosports.transactions.injuries.notes.keyword.exclusions"
	)
	,'')),
	'[ ]+|^[/|,]',' '))) as injury
, count(*) as count
from "prosports.transactions.injuries.extended" inj
where inj.osiics_body_area = ''
AND injury != ''
AND inj.osiics_body_area = ''
AND inj.osiics_tissue_pathology = ''
AND inj.osiics_organ_system = ''
AND inj.osiics_tissue_pathology = ''
group by injury
order by count desc



-- Identify Type
UPDATE "prosports.transactions.injuries.extended" AS t
SET osiics_type = a.osiics_type
FROM
(
	SELECT 
		regexp_substr(
		i.notes,
		(
			SELECT '\b(' || group_concat(type,'|') || ')\b' AS types
			FROM
			(
				SELECT lower(replace(type,"/","|")) as type
				FROM "osiics.v14.ioc"
				GROUP BY type
			)	
		)) as osiics_type, i.transaction_id
	FROM "prosports.transactions.injuries.extended" i
--	WHERE i.transaction_date > '2020-01-01'
	WHERE regexp_like(lower(notes), 
		(
			SELECT '\b(' || group_concat(type,'|') || ')\b' AS types
			FROM
			(
				SELECT lower(replace(type,"/","|")) as type
				FROM "osiics.v14.ioc"
				GROUP BY type
			)	
	))
) a
WHERE t.transaction_id = a.transaction_id 

-- REQUIRED TO MATCH ON " ARM "
update "osiics.v14.ioc"
set region = 'Arm'
where region = 'Upper Arm'

-- UPDATE INJURY FIELD
UPDATE "prosports.transactions.injuries.extended" AS i
SET injury = j.injury
FROM (
	SELECT transaction_id,
		trim(
			regexp_replace(
				trim(
					regexp_replace(
						notes,
						'\ba\b|\bDTD\b|\bdue\b|\bfor\b|\bfrom\b|\bIL\b|\bIR\b|\bon\b|\bplaced\b|\bto\b|\brepair\b|\brest\b|\brecovering\b|\bwith\b|\bleft\b|\bright\b|\binjury\b|\bin\b|\(.*\)',
						''
					)
				),
				'[ ]{2,}|[\)]',
				' '
			)
		)
		 as injury
	FROM "prosports.transactions.injuries.extended"
	WHERE notes != ''
	AND injury is NULL
) AS j
WHERE i.transaction_id = j.transaction_id


-----UPDATE INJURIES BASED ON OSIICS
UPDATE "prosports.transactions.injuries.extended" AS inj
SET osiics_body_area = sub.osiics
FROM (
	SELECT DISTINCT
	  i.transaction_id,
	  i.relinquished,
	  i.notes,
	  group_concat(c.osiics, ', ') OVER (PARTITION BY i.transaction_id) AS osiics
	FROM
	  "prosports.transactions.injuries.extended" i
	JOIN
	  "osiics.v14.ioc.categories" c ON (lower(i.notes) REGEXP lower('\b' || c.regex || '\b'))
	WHERE
	  c.regex != '' AND
	  c.category = 'Body_Area' AND
	  i.osiics_body_area = ''
) as sub
WHERE sub.transaction_id = inj.transaction_id
