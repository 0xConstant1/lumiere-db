BEGIN;
SET LOCAL synchronous_commit = off;
SET LOCAL lock_timeout = '{{swap_lock_timeout}}';

LOCK TABLE
  titles,
  title_search,
  discover_core,
  discover_genre,
  titles_next,
  title_search_next,
  discover_core_next,
  discover_genre_next
IN ACCESS EXCLUSIVE MODE;

DROP TABLE IF EXISTS titles_prev;
DROP TABLE IF EXISTS title_search_prev;
DROP TABLE IF EXISTS discover_core_prev;
DROP TABLE IF EXISTS discover_genre_prev;

ALTER TABLE titles RENAME TO titles_prev;
ALTER TABLE title_search RENAME TO title_search_prev;
ALTER TABLE discover_core RENAME TO discover_core_prev;
ALTER TABLE discover_genre RENAME TO discover_genre_prev;

ALTER TABLE titles_next RENAME TO titles;
ALTER TABLE title_search_next RENAME TO title_search;
ALTER TABLE discover_core_next RENAME TO discover_core;
ALTER TABLE discover_genre_next RENAME TO discover_genre;

DROP TABLE titles_prev;
DROP TABLE title_search_prev;
DROP TABLE discover_core_prev;
DROP TABLE discover_genre_prev;

ALTER TABLE titles RENAME CONSTRAINT titles_next_pkey TO titles_pkey;
ALTER TABLE title_search RENAME CONSTRAINT title_search_next_pkey TO title_search_pkey;

ALTER INDEX IF EXISTS idx_titles_next_type_year RENAME TO idx_titles_type_year;
ALTER INDEX IF EXISTS idx_titles_next_genres_gin RENAME TO idx_titles_genres_gin;
ALTER INDEX IF EXISTS idx_titles_next_type_votes RENAME TO idx_titles_type_votes;
ALTER INDEX IF EXISTS idx_titles_next_tvseries_votes RENAME TO idx_titles_tv_votes;
ALTER INDEX IF EXISTS idx_discover_core_next_group_votes_tconst RENAME TO idx_discover_core_group_votes_tconst;
ALTER INDEX IF EXISTS idx_discover_core_next_genres_gin_build RENAME TO idx_discover_core_genres_gin;
ALTER INDEX IF EXISTS idx_discover_core_next_group_rating_votes_tconst RENAME TO idx_discover_core_group_rating_votes_tconst;
ALTER INDEX IF EXISTS idx_discover_core_next_group_newest_votes_tconst RENAME TO idx_discover_core_group_newest_votes_tconst;
ALTER INDEX IF EXISTS idx_discover_core_next_group_oldest_votes_tconst RENAME TO idx_discover_core_group_oldest_votes_tconst;
ALTER INDEX IF EXISTS idx_discover_genre_next_group_genre_tconst RENAME TO idx_discover_genre_group_genre_tconst;
ALTER INDEX IF EXISTS title_search_next_bm25 RENAME TO title_search_bm25;
COMMIT;
