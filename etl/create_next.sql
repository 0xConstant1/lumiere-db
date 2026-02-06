DROP TABLE IF EXISTS titles_next;
CREATE TABLE titles_next (
  tconst TEXT PRIMARY KEY,
  title_type TEXT,
  primary_title TEXT,
  original_title TEXT,
  start_year INT,
  end_year INT,
  is_adult BOOLEAN,
  runtime_minutes INT,
  genres TEXT[],
  average_rating NUMERIC,
  num_votes INT,
  data JSONB NOT NULL,
  dataset_date DATE NOT NULL,
  schema_version INT NOT NULL
);
ALTER TABLE titles_next SET (autovacuum_enabled = false);

DROP TABLE IF EXISTS title_search_next;
CREATE TABLE title_search_next (
  tconst TEXT PRIMARY KEY,
  title_type TEXT,
  start_year INT,
  primary_title TEXT,
  original_title TEXT,
  aka_titles TEXT[],
  popularity INT
);
ALTER TABLE title_search_next SET (autovacuum_enabled = false);

DROP TABLE IF EXISTS discover_core_next;
CREATE TABLE discover_core_next (
  tconst TEXT,
  title_type TEXT,
  type_group TEXT,
  primary_title TEXT,
  original_title TEXT,
  start_year INT,
  end_year INT,
  genres TEXT[],
  average_rating NUMERIC,
  num_votes INT
);
ALTER TABLE discover_core_next SET (autovacuum_enabled = false);

DROP TABLE IF EXISTS discover_genre_next;
CREATE TABLE discover_genre_next (
  tconst TEXT,
  title_type TEXT,
  type_group TEXT,
  primary_title TEXT,
  original_title TEXT,
  start_year INT,
  end_year INT,
  genres TEXT[],
  average_rating NUMERIC,
  num_votes INT,
  genre TEXT
);
ALTER TABLE discover_genre_next SET (autovacuum_enabled = false);
