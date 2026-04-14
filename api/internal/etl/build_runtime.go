package etl

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
			return fmt.Errorf("fetch title batch after %q: %w", last, err)
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
			return fmt.Errorf("build batch %d: %w", batchNum, err)
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
		return nil, fmt.Errorf("query title batch: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var tconst string
		if err := rows.Scan(&tconst); err != nil {
			return nil, fmt.Errorf("scan title batch: %w", err)
		}
		out = append(out, tconst)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate title batch: %w", err)
	}
	return out, nil
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
		return fmt.Errorf("fetch batch data: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin batch transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	copyResult, err := copyBatchRows(ctx, tx, cfg, tconsts, datasetDate, data)
	if err != nil {
		return fmt.Errorf("copy batch rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	logger.Printf("etl: batch inserted titles=%d search=%d", copyResult.titlesInserted, copyResult.searchInserted)
	return nil
}
