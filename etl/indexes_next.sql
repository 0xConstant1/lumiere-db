CREATE INDEX IF NOT EXISTS idx_titles_next_type_year ON titles_next (title_type, start_year);
CREATE INDEX IF NOT EXISTS idx_titles_next_genres_gin ON titles_next USING GIN (genres);
CREATE INDEX IF NOT EXISTS idx_titles_next_type_votes ON titles_next (title_type, num_votes DESC);

-- Optional: narrow index for TV series discovery
CREATE INDEX IF NOT EXISTS idx_titles_next_tvseries_votes
  ON titles_next (start_year, num_votes DESC)
  WHERE title_type IN ('tvseries', 'tvminiseries', 'tvspecial');

CREATE INDEX IF NOT EXISTS idx_discover_core_next_group_votes_tconst
  ON discover_core_next (type_group, num_votes DESC NULLS LAST, tconst DESC);

CREATE INDEX IF NOT EXISTS idx_discover_core_next_genres_gin
  ON discover_core_next USING GIN (genres);

CREATE INDEX IF NOT EXISTS idx_discover_core_next_group_rating_votes_tconst
  ON discover_core_next (type_group, average_rating DESC NULLS LAST, num_votes DESC NULLS LAST, tconst DESC);

CREATE INDEX IF NOT EXISTS idx_discover_core_next_group_newest_votes_tconst
  ON discover_core_next (type_group, start_year DESC NULLS LAST, num_votes DESC NULLS LAST, tconst DESC);

CREATE INDEX IF NOT EXISTS idx_discover_core_next_group_oldest_votes_tconst
  ON discover_core_next (type_group, start_year ASC NULLS LAST, num_votes DESC NULLS LAST, tconst DESC);

CREATE INDEX IF NOT EXISTS idx_discover_genre_next_group_genre_tconst
  ON discover_genre_next (type_group, genre, tconst);
