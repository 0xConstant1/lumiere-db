DROP TABLE IF EXISTS stg_title_basics;
CREATE UNLOGGED TABLE stg_title_basics (
  tconst TEXT,
  titleType TEXT,
  primaryTitle TEXT,
  originalTitle TEXT,
  isAdult BOOLEAN,
  startYear INT,
  endYear INT,
  runtimeMinutes INT,
  genres TEXT
);
ALTER TABLE stg_title_basics SET (autovacuum_enabled = false);
TRUNCATE stg_title_basics;

DROP TABLE IF EXISTS stg_title_akas;
CREATE UNLOGGED TABLE stg_title_akas (
  titleId TEXT,
  ordering INT,
  title TEXT,
  region TEXT,
  language TEXT,
  types TEXT,
  attributes TEXT,
  isOriginalTitle BOOLEAN
);
ALTER TABLE stg_title_akas SET (autovacuum_enabled = false);
TRUNCATE stg_title_akas;

DROP TABLE IF EXISTS stg_title_ratings;
CREATE UNLOGGED TABLE stg_title_ratings (
  tconst TEXT,
  averageRating DOUBLE PRECISION,
  numVotes INT
);
ALTER TABLE stg_title_ratings SET (autovacuum_enabled = false);
TRUNCATE stg_title_ratings;

DROP TABLE IF EXISTS stg_title_principals;
CREATE UNLOGGED TABLE stg_title_principals (
  tconst TEXT,
  ordering INT,
  nconst TEXT,
  category TEXT,
  characters TEXT
);
ALTER TABLE stg_title_principals SET (autovacuum_enabled = false);
TRUNCATE stg_title_principals;

DROP TABLE IF EXISTS stg_title_crew;
CREATE UNLOGGED TABLE stg_title_crew (
  tconst TEXT,
  directors TEXT,
  writers TEXT
);
ALTER TABLE stg_title_crew SET (autovacuum_enabled = false);
TRUNCATE stg_title_crew;

DROP TABLE IF EXISTS stg_title_episode;
CREATE UNLOGGED TABLE stg_title_episode (
  tconst TEXT,
  parentTconst TEXT,
  seasonNumber INT,
  episodeNumber INT
);
ALTER TABLE stg_title_episode SET (autovacuum_enabled = false);
TRUNCATE stg_title_episode;

DROP TABLE IF EXISTS stg_name_basics;
CREATE UNLOGGED TABLE stg_name_basics (
  nconst TEXT,
  primaryName TEXT
);
ALTER TABLE stg_name_basics SET (autovacuum_enabled = false);
TRUNCATE stg_name_basics;
