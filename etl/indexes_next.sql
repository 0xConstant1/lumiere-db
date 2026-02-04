CREATE INDEX IF NOT EXISTS idx_titles_next_type_year ON titles_next (title_type, start_year);
CREATE INDEX IF NOT EXISTS idx_titles_next_genres_gin ON titles_next USING GIN (genres);
CREATE INDEX IF NOT EXISTS idx_titles_next_type_votes ON titles_next (title_type, num_votes DESC);

-- Optional: narrow index for TV series discovery
CREATE INDEX IF NOT EXISTS idx_titles_next_tvseries_votes
  ON titles_next (start_year, num_votes DESC)
  WHERE title_type = 'tvseries';
