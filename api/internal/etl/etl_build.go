package etl

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"sort"
	"strings"
	"time"
)

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type searchCopyRow struct {
	Tconst        string
	TitleType     string
	StartYear     *int
	PrimaryTitle  string
	OriginalTitle string
	AkaTitles     []string
	Popularity    int
}

type titlesNextCopySource struct {
	cfg              Config
	datasetDate      time.Time
	tconsts          []string
	basics           map[string]TitleBasics
	ratings          map[string]Rating
	akasByTitle      map[string]map[string][]Aka
	actorsByTitle    map[string][]principalRow
	producersByTitle map[string][]principalRow
	crewByTitle      map[string]crewLists
	episodesByTitle  map[string][]Season
	names            map[string]string
	searchRows       *[]searchCopyRow

	idx      int
	inserted int
	row      []any
	err      error
}

func newTitlesNextCopySource(
	cfg Config,
	datasetDate time.Time,
	tconsts []string,
	basics map[string]TitleBasics,
	ratings map[string]Rating,
	akasByTitle map[string]map[string][]Aka,
	actorsByTitle map[string][]principalRow,
	producersByTitle map[string][]principalRow,
	crewByTitle map[string]crewLists,
	episodesByTitle map[string][]Season,
	names map[string]string,
	searchRows *[]searchCopyRow,
) *titlesNextCopySource {
	return &titlesNextCopySource{
		cfg:              cfg,
		datasetDate:      datasetDate,
		tconsts:          tconsts,
		basics:           basics,
		ratings:          ratings,
		akasByTitle:      akasByTitle,
		actorsByTitle:    actorsByTitle,
		producersByTitle: producersByTitle,
		crewByTitle:      crewByTitle,
		episodesByTitle:  episodesByTitle,
		names:            names,
		searchRows:       searchRows,
		row:              make([]any, 14),
	}
}

func (s *titlesNextCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	for s.idx < len(s.tconsts) {
		tconst := s.tconsts[s.idx]
		s.idx++

		basic, ok := s.basics[tconst]
		if !ok {
			continue
		}

		rating := s.ratings[tconst]
		akas := s.akasByTitle[tconst]
		if akas == nil {
			akas = map[string][]Aka{}
		} else {
			akas = filterAkas(akas, basic.PrimaryTitle, basic.OriginalTitle)
			if akas == nil {
				akas = map[string][]Aka{}
			}
		}

		cast := buildCast(s.actorsByTitle[tconst], s.names)
		if cast == nil {
			cast = []CastMember{}
		}

		producers := buildProducers(s.producersByTitle[tconst], s.names)
		if producers == nil {
			producers = []Producer{}
		}

		crewLists := s.crewByTitle[tconst]
		directors := buildCrewMembers(crewLists.Directors, s.names)
		writers := buildCrewMembers(crewLists.Writers, s.names)
		if directors == nil {
			directors = []CrewMember{}
		}
		if writers == nil {
			writers = []CrewMember{}
		}

		episodes := s.episodesByTitle[tconst]
		if episodes == nil {
			episodes = []Season{}
		}

		data := TitleData{
			Basics:  basic,
			Akas:    akas,
			Ratings: rating,
			Cast:    cast,
			Crew: TitleCrew{
				Directors: directors,
				Writers:   writers,
				Producers: producers,
			},
			Episodes: episodes,
		}

		jsonb, err := toJSONB(data)
		if err != nil {
			s.err = fmt.Errorf("jsonb %s: %w", tconst, err)
			return false
		}

		s.row[0] = tconst
		s.row[1] = basic.TitleType
		s.row[2] = basic.PrimaryTitle
		s.row[3] = basic.OriginalTitle
		s.row[4] = intOrNil(basic.StartYear)
		s.row[5] = intOrNil(basic.EndYear)
		s.row[6] = basic.IsAdult
		s.row[7] = intOrNil(basic.RuntimeMinutes)
		s.row[8] = basic.Genres
		s.row[9] = floatOrNil(rating.AverageRating)
		s.row[10] = intOrNil(rating.NumVotes)
		s.row[11] = jsonb
		s.row[12] = s.datasetDate
		s.row[13] = s.cfg.SchemaVersion

		if basic.TitleType != "tvepisode" {
			akaTitles := make([]string, 0)
			for _, regionEntries := range akas {
				for _, aka := range regionEntries {
					akaTitles = append(akaTitles, aka.Title)
				}
			}
			akaTitles = dedupeStrings(akaTitles)
			*s.searchRows = append(*s.searchRows, searchCopyRow{
				Tconst:        tconst,
				TitleType:     basic.TitleType,
				StartYear:     basic.StartYear,
				PrimaryTitle:  basic.PrimaryTitle,
				OriginalTitle: basic.OriginalTitle,
				AkaTitles:     akaTitles,
				Popularity:    computeTitlePopularity(rating.NumVotes, basic.StartYear, s.datasetDate.Year()),
			})
		}

		s.inserted++
		return true
	}
	return false
}

func (s *titlesNextCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *titlesNextCopySource) Err() error {
	return s.err
}

func (s *titlesNextCopySource) Inserted() int {
	return s.inserted
}

type searchRowsCopySource struct {
	rows     []searchCopyRow
	idx      int
	inserted int
	row      []any
}

func newSearchRowsCopySource(rows []searchCopyRow) *searchRowsCopySource {
	return &searchRowsCopySource{
		rows: rows,
		row:  make([]any, 7),
	}
}

func (s *searchRowsCopySource) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	item := s.rows[s.idx]
	s.idx++

	s.row[0] = item.Tconst
	s.row[1] = item.TitleType
	s.row[2] = intOrNil(item.StartYear)
	s.row[3] = item.PrimaryTitle
	s.row[4] = item.OriginalTitle
	s.row[5] = item.AkaTitles
	s.row[6] = item.Popularity

	s.inserted++
	return true
}

func (s *searchRowsCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *searchRowsCopySource) Err() error {
	return nil
}

func (s *searchRowsCopySource) Inserted() int {
	return s.inserted
}

func buildTitlesInBatches(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	datasetDate, err := time.Parse("2006-01-02", cfg.DatasetDate)
	if err != nil {
		return fmt.Errorf("dataset_date: %w", err)
	}

	last := ""
	batchNum := 0
	for {
		tconsts, err := fetchTconstBatch(ctx, pool, last, cfg.BatchSize)
		if err != nil {
			return err
		}
		if len(tconsts) == 0 {
			break
		}

		batchNum++
		logger.Printf("etl: batch %d tconsts=%d", batchNum, len(tconsts))

		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire conn: %w", err)
		}
		err = buildAndInsertBatch(ctx, conn, cfg, tconsts, datasetDate, logger)
		conn.Release()
		if err != nil {
			return err
		}

		last = tconsts[len(tconsts)-1]
	}

	logger.Printf("etl: batch build complete (%d batches)", batchNum)
	return nil
}

func fetchTconstBatch(ctx context.Context, pool *pgxpool.Pool, last string, limit int) ([]string, error) {
	rows, err := pool.Query(ctx, `
SELECT tconst
FROM title_filter
WHERE tconst > $1
ORDER BY tconst
LIMIT $2`, last, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var tconst string
		if err := rows.Scan(&tconst); err != nil {
			return nil, err
		}
		out = append(out, tconst)
	}
	return out, rows.Err()
}

type batchFetchData struct {
	basics           map[string]TitleBasics
	ratings          map[string]Rating
	akasByTitle      map[string]map[string][]Aka
	actorsByTitle    map[string][]principalRow
	producersByTitle map[string][]principalRow
	crewByTitle      map[string]crewLists
	episodesByTitle  map[string][]Season
	names            map[string]string
}

type batchCopyResult struct {
	titlesInserted int
	searchInserted int
}

func fetchBatchData(ctx context.Context, conn *pgxpool.Conn, cfg Config, tconsts []string) (batchFetchData, error) {
	var data batchFetchData

	basics, err := fetchBasics(ctx, conn, tconsts)
	if err != nil {
		return data, err
	}

	seriesTconsts := make([]string, 0, len(basics))
	for _, basic := range basics {
		if basic.TitleType == "tvseries" || basic.TitleType == "tvminiseries" {
			seriesTconsts = append(seriesTconsts, basic.Tconst)
		}
	}

	ratings, err := fetchRatings(ctx, conn, tconsts)
	if err != nil {
		return data, err
	}

	akasByTitle, err := fetchAkas(ctx, conn, tconsts)
	if err != nil {
		return data, err
	}

	principals, err := fetchPrincipals(ctx, conn, tconsts, cfg.MaxActors, cfg.MaxProducers)
	if err != nil {
		return data, err
	}

	actorsByTitle := make(map[string][]principalRow)
	producersByTitle := make(map[string][]principalRow)
	for _, row := range principals {
		if row.Category == "actor" || row.Category == "actress" {
			actorsByTitle[row.Tconst] = append(actorsByTitle[row.Tconst], row)
		} else {
			producersByTitle[row.Tconst] = append(producersByTitle[row.Tconst], row)
		}
	}

	if cfg.MaxActors == 0 {
		actorsByTitle = map[string][]principalRow{}
	}
	if cfg.MaxProducers == 0 {
		producersByTitle = map[string][]principalRow{}
	}

	for tconst, list := range actorsByTitle {
		if len(list) > cfg.MaxActors {
			actorsByTitle[tconst] = list[:cfg.MaxActors]
		}
	}
	for tconst, list := range producersByTitle {
		if len(list) > cfg.MaxProducers {
			producersByTitle[tconst] = list[:cfg.MaxProducers]
		}
	}

	crewByTitle, err := fetchCrew(ctx, conn, tconsts)
	if err != nil {
		return data, err
	}

	for tconst, lists := range crewByTitle {
		if cfg.MaxDirectors == 0 {
			lists.Directors = nil
		} else if len(lists.Directors) > cfg.MaxDirectors {
			lists.Directors = lists.Directors[:cfg.MaxDirectors]
		}
		if cfg.MaxWriters == 0 {
			lists.Writers = nil
		} else if len(lists.Writers) > cfg.MaxWriters {
			lists.Writers = lists.Writers[:cfg.MaxWriters]
		}
		crewByTitle[tconst] = lists
	}

	nconstSet := make(map[string]struct{})
	for _, list := range actorsByTitle {
		for _, row := range list {
			nconstSet[row.Nconst] = struct{}{}
		}
	}
	for _, list := range producersByTitle {
		for _, row := range list {
			nconstSet[row.Nconst] = struct{}{}
		}
	}
	for _, list := range crewByTitle {
		for _, nconst := range list.Directors {
			nconstSet[nconst] = struct{}{}
		}
		for _, nconst := range list.Writers {
			nconstSet[nconst] = struct{}{}
		}
	}

	nconsts := make([]string, 0, len(nconstSet))
	for nconst := range nconstSet {
		nconsts = append(nconsts, nconst)
	}

	names, err := fetchNames(ctx, conn, nconsts)
	if err != nil {
		return data, err
	}

	episodesByTitle, err := fetchEpisodes(ctx, conn, seriesTconsts)
	if err != nil {
		return data, err
	}

	data = batchFetchData{
		basics:           basics,
		ratings:          ratings,
		akasByTitle:      akasByTitle,
		actorsByTitle:    actorsByTitle,
		producersByTitle: producersByTitle,
		crewByTitle:      crewByTitle,
		episodesByTitle:  episodesByTitle,
		names:            names,
	}
	return data, nil
}

func copyBatchRows(
	ctx context.Context,
	tx pgx.Tx,
	cfg Config,
	tconsts []string,
	datasetDate time.Time,
	data batchFetchData,
) (batchCopyResult, error) {
	var out batchCopyResult

	searchRows := make([]searchCopyRow, 0, len(tconsts))
	titlesSource := newTitlesNextCopySource(
		cfg,
		datasetDate,
		tconsts,
		data.basics,
		data.ratings,
		data.akasByTitle,
		data.actorsByTitle,
		data.producersByTitle,
		data.crewByTitle,
		data.episodesByTitle,
		data.names,
		&searchRows,
	)

	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"titles_next"},
		[]string{
			"tconst",
			"title_type",
			"primary_title",
			"original_title",
			"start_year",
			"end_year",
			"is_adult",
			"runtime_minutes",
			"genres",
			"average_rating",
			"num_votes",
			"data",
			"dataset_date",
			"schema_version",
		},
		titlesSource,
	)
	if err != nil {
		return out, fmt.Errorf("copy titles_next: %w", err)
	}
	if err := titlesSource.Err(); err != nil {
		return out, err
	}

	if len(searchRows) > 0 {
		searchSource := newSearchRowsCopySource(searchRows)
		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"title_search_next"},
			[]string{
				"tconst",
				"title_type",
				"start_year",
				"primary_title",
				"original_title",
				"aka_titles",
				"popularity",
			},
			searchSource,
		)
		if err != nil {
			return out, fmt.Errorf("copy title_search_next: %w", err)
		}
	}

	out.titlesInserted = titlesSource.Inserted()
	out.searchInserted = len(searchRows)
	return out, nil
}

func buildAndInsertBatch(ctx context.Context, conn *pgxpool.Conn, cfg Config, tconsts []string, datasetDate time.Time, logger *log.Logger) error {
	data, err := fetchBatchData(ctx, conn, cfg, tconsts)
	if err != nil {
		return err
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	copyResult, err := copyBatchRows(ctx, tx, cfg, tconsts, datasetDate, data)
	if err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	logger.Printf("etl: batch inserted titles=%d search=%d", copyResult.titlesInserted, copyResult.searchInserted)
	return nil
}

func fetchBasics(ctx context.Context, q queryer, tconsts []string) (map[string]TitleBasics, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
FROM stg_title_basics
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]TitleBasics, len(tconsts))
	for rows.Next() {
		var (
			tconst        string
			titleType     string
			primaryTitle  string
			originalTitle string
			isAdult       pgtype.Bool
			startYear     pgtype.Int4
			endYear       pgtype.Int4
			runtime       pgtype.Int4
			genres        string
		)
		if err := rows.Scan(&tconst, &titleType, &primaryTitle, &originalTitle, &isAdult, &startYear, &endYear, &runtime, &genres); err != nil {
			return nil, err
		}

		titleType = strings.ToLower(strings.TrimSpace(titleType))
		primaryTitle = strings.TrimSpace(primaryTitle)
		originalTitle = strings.TrimSpace(originalTitle)
		out[tconst] = TitleBasics{
			Tconst:         tconst,
			TitleType:      titleType,
			PrimaryTitle:   primaryTitle,
			OriginalTitle:  originalTitle,
			IsAdult:        isAdult.Valid && isAdult.Bool,
			StartYear:      intPtrFromPg(startYear),
			EndYear:        intPtrFromPg(endYear),
			RuntimeMinutes: intPtrFromPg(runtime),
			Genres:         splitList(genres),
		}
	}
	return out, rows.Err()
}

func fetchRatings(ctx context.Context, q queryer, tconsts []string) (map[string]Rating, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, averageRating, numVotes
FROM stg_title_ratings
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]Rating, len(tconsts))
	for rows.Next() {
		var (
			tconst string
			avg    pgtype.Float8
			votes  pgtype.Int4
		)
		if err := rows.Scan(&tconst, &avg, &votes); err != nil {
			return nil, err
		}
		out[tconst] = Rating{
			AverageRating: floatPtrFromPg(avg),
			NumVotes:      intPtrFromPg(votes),
		}
	}
	return out, rows.Err()
}

func fetchAkas(ctx context.Context, q queryer, tconsts []string) (map[string]map[string][]Aka, error) {
	rows, err := q.Query(ctx, `
SELECT titleId, ordering, title, region, language, types, attributes, isOriginalTitle
FROM stg_title_akas
WHERE titleId = ANY($1)
ORDER BY titleId, ordering`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]map[string][]Aka)
	for rows.Next() {
		var (
			titleId         string
			ordering        pgtype.Int4
			title           string
			region          string
			language        string
			types           string
			attributes      string
			isOriginalTitle pgtype.Bool
		)
		if err := rows.Scan(&titleId, &ordering, &title, &region, &language, &types, &attributes, &isOriginalTitle); err != nil {
			return nil, err
		}

		region = strings.TrimSpace(nullIfNA(region))
		if region == "" {
			region = "GLOBAL"
		}
		orderVal := 0
		if ordering.Valid {
			orderVal = int(ordering.Int32)
		}

		aka := Aka{
			Title:           title,
			Language:        strings.TrimSpace(nullIfNA(language)),
			Types:           splitList(types),
			Attributes:      splitList(attributes),
			IsOriginalTitle: isOriginalTitle.Valid && isOriginalTitle.Bool,
			Ordering:        orderVal,
		}

		if _, ok := out[titleId]; !ok {
			out[titleId] = make(map[string][]Aka)
		}
		out[titleId][region] = append(out[titleId][region], aka)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, byRegion := range out {
		for region, entries := range byRegion {
			sort.SliceStable(entries, func(i, j int) bool {
				a := entries[i]
				b := entries[j]
				aRank := akaTypeRank(a)
				bRank := akaTypeRank(b)
				if aRank != bRank {
					return aRank < bRank
				}
				if a.IsOriginalTitle != b.IsOriginalTitle {
					return a.IsOriginalTitle
				}
				aOrd := akaOrderingOrMax(a.Ordering)
				bOrd := akaOrderingOrMax(b.Ordering)
				if aOrd != bOrd {
					return aOrd < bOrd
				}
				return strings.ToLower(a.Title) < strings.ToLower(b.Title)
			})
			byRegion[region] = entries
		}
	}

	return out, nil
}

func fetchPrincipals(ctx context.Context, q queryer, tconsts []string, maxActors int, maxProducers int) ([]principalRow, error) {
	if maxActors <= 0 && maxProducers <= 0 {
		return []principalRow{}, nil
	}

	rows, err := q.Query(ctx, `
WITH ranked AS (
  SELECT tconst, ordering, nconst, category, characters,
         CASE WHEN category IN ('actor','actress') THEN 1 ELSE 0 END AS is_actor,
         row_number() OVER (
           PARTITION BY tconst, CASE WHEN category IN ('actor','actress') THEN 1 ELSE 0 END
           ORDER BY ordering
         ) AS rn
  FROM stg_title_principals
  WHERE tconst = ANY($1)
    AND category IN (
      'actor','actress',
      'producer','executive_producer','associate_producer','co_producer','line_producer'
    )
)
SELECT tconst, ordering, nconst, category, characters
FROM ranked
WHERE (is_actor = 1 AND rn <= $2)
   OR (is_actor = 0 AND rn <= $3)
ORDER BY tconst, ordering`, tconsts, maxActors, maxProducers)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]principalRow, 0)
	for rows.Next() {
		var (
			tconst     string
			ordering   pgtype.Int4
			nconst     string
			category   string
			characters string
		)
		if err := rows.Scan(&tconst, &ordering, &nconst, &category, &characters); err != nil {
			return nil, err
		}
		ord := 0
		if ordering.Valid {
			ord = int(ordering.Int32)
		}
		out = append(out, principalRow{
			Tconst:     tconst,
			Ordering:   ord,
			Nconst:     nconst,
			Category:   category,
			Characters: characters,
		})
	}
	return out, rows.Err()
}

func fetchCrew(ctx context.Context, q queryer, tconsts []string) (map[string]crewLists, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, directors, writers
FROM stg_title_crew
WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]crewLists)
	for rows.Next() {
		var tconst, directors, writers string
		if err := rows.Scan(&tconst, &directors, &writers); err != nil {
			return nil, err
		}
		out[tconst] = crewLists{
			Directors: splitList(directors),
			Writers:   splitList(writers),
		}
	}
	return out, rows.Err()
}

func fetchNames(ctx context.Context, q queryer, nconsts []string) (map[string]string, error) {
	if len(nconsts) == 0 {
		return map[string]string{}, nil
	}

	rows, err := q.Query(ctx, `
SELECT nconst, primaryName
FROM stg_name_basics
WHERE nconst = ANY($1)`, nconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string, len(nconsts))
	for rows.Next() {
		var nconst, name string
		if err := rows.Scan(&nconst, &name); err != nil {
			return nil, err
		}
		out[nconst] = name
	}
	return out, rows.Err()
}

func fetchEpisodes(ctx context.Context, q queryer, tconsts []string) (map[string][]Season, error) {
	if len(tconsts) == 0 {
		return map[string][]Season{}, nil
	}

	rows, err := q.Query(ctx, `
SELECT parent_tconst, tconst, season_number, episode_number,
       primary_title, start_year, average_rating, num_votes
FROM stg_episode_enriched
WHERE parent_tconst = ANY($1)
ORDER BY parent_tconst, season_number NULLS LAST, episode_number NULLS LAST, tconst`, tconsts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byParent := make(map[string]map[seasonKey]*Season)
	for rows.Next() {
		var (
			parentTconst string
			tconst       string
			seasonNum    pgtype.Int4
			episodeNum   pgtype.Int4
			primaryTitle string
			startYear    pgtype.Int4
			avgRating    pgtype.Float8
			numVotes     pgtype.Int4
		)
		if err := rows.Scan(&parentTconst, &tconst, &seasonNum, &episodeNum, &primaryTitle, &startYear, &avgRating, &numVotes); err != nil {
			return nil, err
		}

		if _, ok := byParent[parentTconst]; !ok {
			byParent[parentTconst] = make(map[seasonKey]*Season)
		}

		key := seasonKeyFromPtr(intPtrFromPg(seasonNum))
		season := byParent[parentTconst][key]
		if season == nil {
			season = &Season{SeasonNumber: intPtrFromPg(seasonNum), Episodes: []Episode{}}
			byParent[parentTconst][key] = season
		}

		season.Episodes = append(season.Episodes, Episode{
			Tconst:        tconst,
			EpisodeNumber: intPtrFromPg(episodeNum),
			PrimaryTitle:  primaryTitle,
			StartYear:     intPtrFromPg(startYear),
			AverageRating: floatPtrFromPg(avgRating),
			NumVotes:      intPtrFromPg(numVotes),
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make(map[string][]Season, len(byParent))
	for parent, seasonsMap := range byParent {
		seasons := make([]Season, 0, len(seasonsMap))
		for _, season := range seasonsMap {
			seasons = append(seasons, *season)
		}
		sort.Slice(seasons, func(i, j int) bool {
			if seasons[i].SeasonNumber == nil && seasons[j].SeasonNumber == nil {
				return false
			}
			if seasons[i].SeasonNumber == nil {
				return false
			}
			if seasons[j].SeasonNumber == nil {
				return true
			}
			return *seasons[i].SeasonNumber < *seasons[j].SeasonNumber
		})
		out[parent] = seasons
	}

	return out, nil
}

func buildCast(rows []principalRow, names map[string]string) []CastMember {
	if len(rows) == 0 {
		return nil
	}
	cast := make([]CastMember, 0, len(rows))
	index := make(map[string]int, len(rows))
	for _, row := range rows {
		key := row.Nconst + "|" + row.Category
		chars := normalizeCharacters(row.Characters)
		if idx, ok := index[key]; ok {
			if len(chars) > 0 {
				cast[idx].Characters = mergeDedupStrings(cast[idx].Characters, chars)
			}
			continue
		}
		cast = append(cast, CastMember{
			Nconst:     row.Nconst,
			Name:       names[row.Nconst],
			Category:   row.Category,
			Characters: chars,
		})
		index[key] = len(cast) - 1
	}
	return cast
}

func mergeDedupStrings(base []string, add []string) []string {
	if len(add) == 0 {
		return base
	}
	if len(base) == 0 {
		return add
	}
	combined := make([]string, 0, len(base)+len(add))
	combined = append(combined, base...)
	combined = append(combined, add...)
	return dedupeCharacters(combined)
}

func dedupeCharacters(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		key := normalizeCharacterKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeCharacterKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	space := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			space = false
			continue
		}
		if !space {
			b.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(b.String())
}

func buildProducers(rows []principalRow, names map[string]string) []Producer {
	if len(rows) == 0 {
		return nil
	}
	out := make([]Producer, 0, len(rows))
	for _, row := range rows {
		out = append(out, Producer{
			Nconst:   row.Nconst,
			Name:     names[row.Nconst],
			Category: row.Category,
		})
	}
	return out
}

func buildCrewMembers(nconsts []string, names map[string]string) []CrewMember {
	if len(nconsts) == 0 {
		return nil
	}
	out := make([]CrewMember, 0, len(nconsts))
	for _, nconst := range nconsts {
		out = append(out, CrewMember{
			Nconst: nconst,
			Name:   names[nconst],
		})
	}
	return out
}

func nullIfNA(val string) string {
	val = strings.TrimSpace(val)
	if val == "\\N" {
		return ""
	}
	return val
}
func intPtrFromPg(val pgtype.Int4) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int32)
	return &v
}

func floatPtrFromPg(val pgtype.Float8) *float64 {
	if !val.Valid {
		return nil
	}
	v := val.Float64
	return &v
}
