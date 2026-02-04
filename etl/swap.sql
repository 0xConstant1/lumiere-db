BEGIN;
SET LOCAL synchronous_commit = off;
TRUNCATE titles;
INSERT INTO titles (
  tconst,
  title_type,
  primary_title,
  original_title,
  start_year,
  end_year,
  is_adult,
  runtime_minutes,
  genres,
  average_rating,
  num_votes,
  data,
  dataset_date,
  schema_version
)
SELECT
  tconst,
  title_type,
  primary_title,
  original_title,
  start_year,
  end_year,
  is_adult,
  runtime_minutes,
  genres,
  average_rating,
  num_votes,
  data,
  dataset_date,
  schema_version
FROM titles_next;

TRUNCATE title_search;
INSERT INTO title_search (
  tconst,
  title_type,
  start_year,
  primary_title,
  original_title,
  aka_titles
)
SELECT
  tconst,
  title_type,
  start_year,
  primary_title,
  original_title,
  aka_titles
FROM title_search_next;

TRUNCATE discover;
INSERT INTO discover (
  tconst,
  title_type,
  primary_title,
  original_title,
  start_year,
  end_year,
  genres,
  average_rating,
  num_votes,
  genre
)
SELECT
  tconst,
  title_type,
  primary_title,
  original_title,
  start_year,
  end_year,
  genres,
  average_rating,
  num_votes,
  genre
FROM discover_next;
COMMIT;
