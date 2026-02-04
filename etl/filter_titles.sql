CREATE UNLOGGED TABLE IF NOT EXISTS title_filter (
  tconst TEXT PRIMARY KEY
);
TRUNCATE title_filter;

INSERT INTO title_filter (tconst)
SELECT tconst
FROM stg_title_basics
WHERE NULLIF(NULLIF(BTRIM(startYear),'\N'),'')::INT = EXTRACT(YEAR FROM '{{dataset_date}}'::DATE)
;

INSERT INTO title_filter (tconst)
SELECT r.tconst
FROM stg_title_ratings r
JOIN stg_title_basics b ON b.tconst = r.tconst
WHERE NULLIF(NULLIF(BTRIM(numVotes),'\N'),'')::INT >= {{min_num_votes}}
ON CONFLICT DO NOTHING;
