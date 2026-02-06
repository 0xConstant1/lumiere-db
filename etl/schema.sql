CREATE TABLE IF NOT EXISTS titles (
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

CREATE TABLE IF NOT EXISTS title_search (
  tconst TEXT PRIMARY KEY,
  title_type TEXT,
  start_year INT,
  primary_title TEXT,
  original_title TEXT,
  aka_titles TEXT[],
  popularity INT
);
ALTER TABLE title_search ADD COLUMN IF NOT EXISTS popularity INT;

CREATE TABLE IF NOT EXISTS discover_core (
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

CREATE TABLE IF NOT EXISTS discover_genre (
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

CREATE TABLE IF NOT EXISTS etl_source_state (
  file_name TEXT PRIMARY KEY,
  last_modified TIMESTAMPTZ NOT NULL,
  content_length BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
