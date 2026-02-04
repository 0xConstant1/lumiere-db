TRUNCATE discover_next;

INSERT INTO discover_next (
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
  '' AS genre
FROM titles_next;

INSERT INTO discover_next (
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
  t.tconst,
  t.title_type,
  t.primary_title,
  t.original_title,
  t.start_year,
  t.end_year,
  t.genres,
  t.average_rating,
  t.num_votes,
  lower(btrim(g.genre))
FROM titles_next t
JOIN LATERAL unnest(t.genres) AS g(genre) ON true;
