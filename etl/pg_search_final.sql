-- Requires the pg_search extension (ParadeDB). This will fail on plain Postgres.
CREATE EXTENSION IF NOT EXISTS pg_search;

CREATE INDEX IF NOT EXISTS title_search_bm25 ON title_search
USING bm25 (
  tconst,
  (primary_title::pdb.icu),
  (original_title::pdb.icu),
  (aka_titles::pdb.icu),
  (title_type::pdb.literal),
  start_year
)
WITH (key_field = 'tconst');
