CREATE INDEX IF NOT EXISTS idx_titles_type_year ON titles (title_type, start_year);
CREATE INDEX IF NOT EXISTS idx_titles_genres_gin ON titles USING GIN (genres);
CREATE INDEX IF NOT EXISTS idx_titles_type_votes ON titles (title_type, num_votes DESC);

-- Optional: narrow index for TV series discovery
CREATE INDEX IF NOT EXISTS idx_titles_tv_votes
  ON titles (start_year, num_votes DESC)
  WHERE title_type IN ('tvseries','tvminiseries','tvspecial');

CREATE INDEX IF NOT EXISTS idx_discover_type_genre_year_votes
  ON discover (title_type, genre, start_year, num_votes DESC);

CREATE INDEX IF NOT EXISTS idx_discover_movie_votes
  ON discover (start_year, num_votes DESC)
  WHERE title_type IN ('movie','tvmovie');
