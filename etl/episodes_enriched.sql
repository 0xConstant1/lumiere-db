DROP TABLE IF EXISTS stg_episode_enriched;
CREATE UNLOGGED TABLE stg_episode_enriched AS
SELECT
  e.parentTconst AS parent_tconst,
  e.tconst AS tconst,
  e.seasonNumber AS season_number,
  e.episodeNumber AS episode_number,
  b.primaryTitle AS primary_title,
  b.startYear AS start_year,
  r.averageRating AS average_rating,
  r.numVotes AS num_votes
FROM stg_title_episode e
JOIN stg_title_basics b ON b.tconst = e.tconst
LEFT JOIN stg_title_ratings r ON r.tconst = e.tconst;
ALTER TABLE stg_episode_enriched SET (autovacuum_enabled = false);

CREATE INDEX IF NOT EXISTS stg_episode_enriched_parent_idx
  ON stg_episode_enriched (parent_tconst);
CREATE INDEX IF NOT EXISTS stg_episode_enriched_parent_order_idx
  ON stg_episode_enriched (parent_tconst, season_number, episode_number, tconst);
