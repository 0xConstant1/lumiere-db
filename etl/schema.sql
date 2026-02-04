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
  aka_titles TEXT[]
);

CREATE TABLE IF NOT EXISTS discover (
  tconst TEXT,
  title_type TEXT,
  primary_title TEXT,
  original_title TEXT,
  start_year INT,
  end_year INT,
  genres TEXT[],
  average_rating NUMERIC,
  num_votes INT,
  genre TEXT
);
