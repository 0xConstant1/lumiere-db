TRUNCATE discover_core_next;
TRUNCATE discover_genre_next;

INSERT INTO discover_core_next (
  tconst,
  title_type,
  type_group,
  primary_title,
  original_title,
  start_year,
  end_year,
  genres,
  average_rating,
  num_votes
)
SELECT
  tconst,
  title_type,
  CASE
    WHEN title_type IN ('movie', 'tvmovie') THEN 'movies'
    WHEN title_type IN ('tvseries', 'tvminiseries', 'tvspecial') THEN 'series'
    ELSE NULL
  END AS type_group,
  primary_title,
  original_title,
  start_year,
  end_year,
  genres,
  average_rating,
  num_votes
FROM titles_next
WHERE title_type IN (
  'movie', 'tvmovie',
  'tvseries', 'tvminiseries', 'tvspecial'
);

INSERT INTO discover_genre_next (
  tconst,
  title_type,
  type_group,
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
  CASE
    WHEN t.title_type IN ('movie', 'tvmovie') THEN 'movies'
    WHEN t.title_type IN ('tvseries', 'tvminiseries', 'tvspecial') THEN 'series'
    ELSE NULL
  END AS type_group,
  t.primary_title,
  t.original_title,
  t.start_year,
  t.end_year,
  t.genres,
  t.average_rating,
  t.num_votes,
  lower(btrim(g.genre))
FROM titles_next t
JOIN LATERAL unnest(t.genres) AS g(genre) ON true
WHERE t.title_type IN (
  'movie', 'tvmovie',
  'tvseries', 'tvminiseries', 'tvspecial'
)
  AND g.genre IS NOT NULL
  AND btrim(g.genre) <> '';
