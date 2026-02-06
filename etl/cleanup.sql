DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'titles_next',
    'title_search_next',
    'discover_core_next',
    'discover_genre_next',
    'title_filter',
    'stg_title_basics',
    'stg_title_akas',
    'stg_title_ratings',
    'stg_title_principals',
    'stg_title_crew',
    'stg_title_episode',
    'stg_episode_enriched',
    'stg_name_basics'
  ]
  LOOP
    IF to_regclass(table_name) IS NOT NULL THEN
      EXECUTE format('TRUNCATE TABLE %I', table_name);
    END IF;
  END LOOP;
END $$;
