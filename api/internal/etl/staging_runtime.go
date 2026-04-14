package etl

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func loadStagingInBatches(ctx context.Context, pool *pgxpool.Pool, cfg Config, logger *log.Logger) error {
	datasetDate, err := time.Parse("2006-01-02", cfg.DatasetDate)
	if err != nil {
		return fmt.Errorf("dataset_date: %w", err)
	}
	datasetYear := datasetDate.Year()

	typeAllowlist := make(map[uint32]struct{}, 11000000)
	currentYearSet := make(map[uint32]struct{}, 250000)
	votedSet := make(map[uint32]struct{}, 4000000)
	var finalAllowlist map[uint32]struct{}
	nameAllowlist := make(map[uint32]struct{}, 4000000)

	allowedTitleTypeRaw := func(line string) bool {
		titleType, ok := fieldByIndex(line, 1)
		if !ok {
			return false
		}
		switch titleType {
		case "movie", "tvMovie", "tvSeries", "tvMiniSeries", "tvSpecial", "tvEpisode":
			return true
		default:
			return false
		}
	}

	allowlistFilter := func(line string, index int, allow map[uint32]struct{}) bool {
		if len(allow) == 0 {
			return false
		}
		raw, ok := fieldByIndex(line, index)
		if !ok {
			return false
		}
		id, ok := parseTconstID(raw)
		if !ok {
			return false
		}
		_, ok = allow[id]
		return ok
	}

	allowedPrincipalCategoryRaw := func(line string) bool {
		category, ok := fieldByIndex(line, 3)
		if !ok {
			return false
		}
		switch category {
		case "actor", "actress",
			"producer", "executive_producer", "associate_producer", "co_producer", "line_producer":
			return true
		default:
			return false
		}
	}

	addNconst := func(raw string) {
		if raw == "" || raw == "\\N" {
			return
		}
		if id, ok := parseNconstID(raw); ok {
			nameAllowlist[id] = struct{}{}
		}
	}

	addNconstList := func(raw string) {
		if raw == "" || raw == "\\N" {
			return
		}
		for part := range strings.SplitSeq(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			addNconst(part)
		}
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Release()

	basicsSpec := tsvLoadSpec{
		File:  "title.basics.tsv.gz",
		Table: "stg_title_basics",
		Columns: []string{
			"tconst", "titletype", "primarytitle", "originaltitle", "isadult", "startyear", "endyear", "runtimeminutes", "genres",
		},
		Types: []tsvColumnType{
			tsvColumnText, tsvColumnText, tsvColumnText, tsvColumnText,
			tsvColumnBool01, tsvColumnInt, tsvColumnInt, tsvColumnInt, tsvColumnText,
		},
		FilterRaw: allowedTitleTypeRaw,
		OnRecord: func(record []string) {
			if len(record) == 0 {
				return
			}
			id, ok := parseTconstID(record[0])
			if !ok {
				return
			}
			typeAllowlist[id] = struct{}{}
			if len(record) > 5 {
				if year := parseInt(record[5]); year != nil && *year == datasetYear {
					currentYearSet[id] = struct{}{}
				}
			}
		},
	}
	path := filepath.Join(cfg.DataDir, basicsSpec.File)
	if err := loadTSVToTable(ctx, conn, path, basicsSpec.Table, basicsSpec.Columns, basicsSpec.Types, basicsSpec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, basicsSpec.Filter, basicsSpec.FilterRaw, basicsSpec.OnRecord, logger); err != nil {
		return err
	}
	logger.Printf("etl: allowlist type=%d current_year=%d", len(typeAllowlist), len(currentYearSet))

	ratingsSpec := tsvLoadSpec{
		File:  "title.ratings.tsv.gz",
		Table: "stg_title_ratings",
		Columns: []string{
			"tconst", "averagerating", "numvotes",
		},
		Types: []tsvColumnType{
			tsvColumnText, tsvColumnFloat, tsvColumnInt,
		},
		FilterRaw: func(line string) bool {
			return allowlistFilter(line, 0, typeAllowlist)
		},
		OnRecord: func(record []string) {
			if len(record) < 3 {
				return
			}
			id, ok := parseTconstID(record[0])
			if !ok {
				return
			}
			if _, ok := typeAllowlist[id]; !ok {
				return
			}
			votes := parseInt(record[2])
			if votes != nil && *votes >= cfg.MinNumVotes {
				votedSet[id] = struct{}{}
			}
		},
	}
	path = filepath.Join(cfg.DataDir, ratingsSpec.File)
	if err := loadTSVToTable(ctx, conn, path, ratingsSpec.Table, ratingsSpec.Columns, ratingsSpec.Types, ratingsSpec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, ratingsSpec.Filter, ratingsSpec.FilterRaw, ratingsSpec.OnRecord, logger); err != nil {
		return err
	}
	logger.Printf("etl: allowlist voted=%d min_votes=%d", len(votedSet), cfg.MinNumVotes)

	finalAllowlist = make(map[uint32]struct{}, len(currentYearSet)+len(votedSet))
	for id := range currentYearSet {
		finalAllowlist[id] = struct{}{}
	}
	for id := range votedSet {
		finalAllowlist[id] = struct{}{}
	}
	logger.Printf("etl: allowlist final=%d", len(finalAllowlist))

	typeAllowlist = nil
	currentYearSet = nil
	votedSet = nil

	specs := []tsvLoadSpec{
		{
			File:  "title.akas.tsv.gz",
			Table: "stg_title_akas",
			Columns: []string{
				"titleid", "ordering", "title", "region", "language", "types", "attributes", "isoriginaltitle",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnInt, tsvColumnText, tsvColumnText,
				tsvColumnText, tsvColumnText, tsvColumnText, tsvColumnBool01,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 0, finalAllowlist)
			},
		},
		{
			File:  "title.principals.tsv.gz",
			Table: "stg_title_principals",
			Columns: []string{
				"tconst", "ordering", "nconst", "category", "characters",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnInt, tsvColumnText, tsvColumnText, tsvColumnText,
			},
			Indexes: []int{0, 1, 2, 3, 5},
			FilterRaw: func(line string) bool {
				if !allowlistFilter(line, 0, finalAllowlist) {
					return false
				}
				return allowedPrincipalCategoryRaw(line)
			},
			OnRecord: func(record []string) {
				if len(record) > 2 {
					addNconst(record[2])
				}
			},
		},
		{
			File:  "title.crew.tsv.gz",
			Table: "stg_title_crew",
			Columns: []string{
				"tconst", "directors", "writers",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText, tsvColumnText,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 0, finalAllowlist)
			},
			OnRecord: func(record []string) {
				if len(record) > 1 {
					addNconstList(record[1])
				}
				if len(record) > 2 {
					addNconstList(record[2])
				}
			},
		},
		{
			File:  "title.episode.tsv.gz",
			Table: "stg_title_episode",
			Columns: []string{
				"tconst", "parenttconst", "seasonnumber", "episodenumber",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText, tsvColumnInt, tsvColumnInt,
			},
			FilterRaw: func(line string) bool {
				return allowlistFilter(line, 1, finalAllowlist)
			},
		},
		{
			File:  "name.basics.tsv.gz",
			Table: "stg_name_basics",
			Columns: []string{
				"nconst", "primaryname",
			},
			Types: []tsvColumnType{
				tsvColumnText, tsvColumnText,
			},
			Indexes: []int{0, 1},
			FilterRaw: func(line string) bool {
				raw, ok := fieldByIndex(line, 0)
				if !ok {
					return false
				}
				id, ok := parseNconstID(raw)
				if !ok {
					return false
				}
				_, ok = nameAllowlist[id]
				return ok
			},
		},
	}

	for _, spec := range specs {
		path := filepath.Join(cfg.DataDir, spec.File)
		if err := loadTSVToTable(ctx, conn, path, spec.Table, spec.Columns, spec.Types, spec.Indexes, cfg.LoadBatchSize, cfg.ReaderBufferSize, spec.Filter, spec.FilterRaw, spec.OnRecord, logger); err != nil {
			return err
		}
	}

	logger.Printf("etl: allowlist names=%d", len(nameAllowlist))

	finalAllowlist = nil
	nameAllowlist = nil
	runtime.GC()
	return nil
}
