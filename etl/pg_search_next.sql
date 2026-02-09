-- Requires the pg_search extension (ParadeDB). This will fail on plain Postgres.
CREATE EXTENSION IF NOT EXISTS pg_search;

CREATE INDEX IF NOT EXISTS title_search_next_bm25 ON title_search_next
USING bm25 (
  tconst,
  (primary_title::pdb.icu('stopwords_language=english', 'ascii_folding=true')),
  (primary_title::pdb.literal_normalized('alias=primary_title_exact', 'ascii_folding=true')),
  (original_title::pdb.icu('stopwords_language=english', 'ascii_folding=true')),
  (original_title::pdb.literal_normalized('alias=original_title_exact', 'ascii_folding=true')),
  (aka_titles::pdb.icu('stopwords_language=english', 'ascii_folding=true')),
  (title_type::pdb.literal),
  start_year,
  popularity
)
WITH (key_field = 'tconst');
