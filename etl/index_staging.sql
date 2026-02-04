CREATE INDEX IF NOT EXISTS stg_title_basics_tconst_idx ON stg_title_basics (tconst);
CREATE INDEX IF NOT EXISTS stg_title_akas_titleid_idx ON stg_title_akas (titleId);
CREATE INDEX IF NOT EXISTS stg_title_principals_tconst_idx ON stg_title_principals (tconst);
CREATE INDEX IF NOT EXISTS stg_name_basics_nconst_idx ON stg_name_basics (nconst);
CREATE INDEX IF NOT EXISTS stg_title_crew_tconst_idx ON stg_title_crew (tconst);
CREATE INDEX IF NOT EXISTS stg_title_episode_parent_idx ON stg_title_episode (parentTconst);
CREATE INDEX IF NOT EXISTS stg_title_ratings_tconst_idx ON stg_title_ratings (tconst);
