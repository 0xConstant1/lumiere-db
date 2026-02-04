DROP TABLE IF EXISTS titles_next;
CREATE UNLOGGED TABLE titles_next (
  tconst TEXT,
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
CREATE UNLOGGED TABLE title_search_next (
  tconst TEXT PRIMARY KEY,
  title_type TEXT,
  start_year INT,
  primary_title TEXT,
  original_title TEXT,
  aka_titles TEXT[]
);
ALTER TABLE title_search_next SET (autovacuum_enabled = false);

DROP TABLE IF EXISTS discover_next;
CREATE UNLOGGED TABLE discover_next (
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
ALTER TABLE discover_next SET (autovacuum_enabled = false);
