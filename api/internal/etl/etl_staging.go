package etl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type tsvLoadSpec struct {
	File      string
	Table     string
	Columns   []string
	Types     []tsvColumnType
	Indexes   []int
	Filter    func([]string) bool
	FilterRaw func(string) bool
	OnRecord  func([]string)
}

type tsvColumnType uint8

const (
	tsvColumnText tsvColumnType = iota
	tsvColumnInt
	tsvColumnFloat
	tsvColumnBool01
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

func loadTSVToTable(ctx context.Context, conn *pgxpool.Conn, path, table string, columns []string, columnTypes []tsvColumnType, indexes []int, batchSize int, readerBuffer int, filter func([]string) bool, filterRaw func(string) bool, onRecord func([]string), logger *log.Logger) error {
	start := time.Now()
	logger.Printf("etl: load %s -> %s", filepath.Base(path), table)

	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("gzip %s: %w", path, err)
	}
	defer gz.Close()

	var reader *bufio.Reader
	if readerBuffer > 0 {
		reader = bufio.NewReaderSize(gz, readerBuffer)
	} else {
		reader = bufio.NewReader(gz)
	}

	source := newTSVCopySource(reader, columns, columnTypes, indexes, batchSize, filter, filterRaw, onRecord, logger, table, start)
	for {
		source.beginBatch()
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{table}, columns, source); err != nil {
			return fmt.Errorf("copy %s: %w", table, err)
		}
		if err := source.Err(); err != nil {
			return err
		}
		if source.Done() {
			break
		}
	}

	logger.Printf(
		"etl: finished %s scanned=%d inserted=%d skipped=%d in %s",
		table,
		source.Scanned(),
		source.Inserted(),
		source.Skipped(),
		time.Since(start).Truncate(time.Millisecond),
	)
	return nil
}

type tsvCopySource struct {
	reader      *bufio.Reader
	columns     []string
	columnTypes []tsvColumnType
	indexes     []int
	filter      func([]string) bool
	filterRaw   func(string) bool
	onRecord    func([]string)
	logger      *log.Logger
	table       string
	start       time.Time

	headerRead bool
	sourceLen  int
	maxIndex   int
	row        []any
	err        error
	fullRecord []string
	record     []string

	scanned  int
	inserted int
	skipped  int

	logEvery int

	batchLimit    int
	batchInserted int
	exhausted     bool
}

func newTSVCopySource(reader *bufio.Reader, columns []string, columnTypes []tsvColumnType, indexes []int, batchSize int, filter func([]string) bool, filterRaw func(string) bool, onRecord func([]string), logger *log.Logger, table string, start time.Time) *tsvCopySource {
	types := columnTypes
	if len(types) != len(columns) {
		types = make([]tsvColumnType, len(columns))
		for i := range types {
			types[i] = tsvColumnText
		}
	}
	maxIdx := -1
	for _, idx := range indexes {
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	return &tsvCopySource{
		reader:      reader,
		columns:     columns,
		columnTypes: types,
		indexes:     indexes,
		filter:      filter,
		filterRaw:   filterRaw,
		onRecord:    onRecord,
		logger:      logger,
		table:       table,
		start:       start,
		maxIndex:    maxIdx,
		row:         make([]any, len(columns)),
		logEvery:    500000,
		batchLimit:  batchSize,
	}
}

func trimLineEnding(line string) string {
	if len(line) == 0 {
		return line
	}
	if line[len(line)-1] == '\n' {
		line = line[:len(line)-1]
	}
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return line
}

func countTSVColumns(line string) int {
	if line == "" {
		return 0
	}
	count := 1
	for i := 0; i < len(line); i++ {
		if line[i] == '\t' {
			count++
		}
	}
	return count
}

func parseTSVRecord(line string, expected int, dst []string) []string {
	if expected <= 0 {
		return dst[:0]
	}
	if cap(dst) < expected {
		dst = make([]string, expected)
	} else {
		dst = dst[:expected]
		for i := range dst {
			dst[i] = ""
		}
	}

	field := 0
	start := 0
	for i := 0; i <= len(line); i++ {
		if i < len(line) && line[i] != '\t' {
			continue
		}
		if field == expected-1 {
			dst[field] = line[start:]
			return dst
		}
		dst[field] = line[start:i]
		field++
		start = i + 1
		if field >= expected {
			return dst
		}
	}
	return dst
}

func convertTSVValue(raw string, typ tsvColumnType) any {
	switch typ {
	case tsvColumnText:
		return raw
	case tsvColumnInt:
		if raw == "" || raw == "\\N" {
			return nil
		}
		val, err := strconv.Atoi(raw)
		if err != nil {
			return nil
		}
		return val
	case tsvColumnFloat:
		if raw == "" || raw == "\\N" {
			return nil
		}
		val, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil
		}
		return val
	case tsvColumnBool01:
		if raw == "" || raw == "\\N" {
			return nil
		}
		if raw == "1" {
			return true
		}
		if raw == "0" {
			return false
		}
		return nil
	default:
		return raw
	}
}

func (s *tsvCopySource) readLine() (string, error) {
	line, err := s.reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if errors.Is(err, io.EOF) && len(line) == 0 {
		return "", io.EOF
	}
	return trimLineEnding(line), err
}

func (s *tsvCopySource) Next() bool {
	if s.err != nil || s.exhausted {
		return false
	}
	if s.batchLimit > 0 && s.batchInserted >= s.batchLimit {
		return false
	}
	for {
		line, err := s.readLine()
		if errors.Is(err, io.EOF) && line == "" {
			s.exhausted = true
			return false
		}
		if err != nil && !errors.Is(err, io.EOF) {
			s.err = err
			return false
		}
		if line == "" {
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		if !s.headerRead {
			s.sourceLen = countTSVColumns(line)
			if len(s.indexes) == 0 && s.sourceLen != len(s.columns) {
				s.err = fmt.Errorf("etl: %s header columns=%d expected=%d", s.table, s.sourceLen, len(s.columns))
				return false
			}
			if len(s.indexes) > 0 && s.maxIndex >= s.sourceLen {
				s.err = fmt.Errorf("etl: %s header columns=%d max_index=%d", s.table, s.sourceLen, s.maxIndex)
				return false
			}
			s.headerRead = true
			if errors.Is(err, io.EOF) {
				s.exhausted = true
				return false
			}
			continue
		}

		s.scanned++
		if s.filterRaw != nil && !s.filterRaw(line) {
			s.skipped++
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		s.fullRecord = parseTSVRecord(line, s.sourceLen, s.fullRecord)

		record := s.fullRecord
		if len(s.indexes) > 0 {
			if cap(s.record) < len(s.columns) {
				s.record = make([]string, len(s.columns))
			} else {
				s.record = s.record[:len(s.columns)]
			}
			for i, idx := range s.indexes {
				val := ""
				if idx >= 0 && idx < len(s.fullRecord) {
					val = s.fullRecord[idx]
				}
				s.record[i] = val
			}
			record = s.record
		}

		if s.filter != nil && !s.filter(record) {
			s.skipped++
			if errors.Is(err, io.EOF) {
				return false
			}
			continue
		}

		if s.onRecord != nil {
			s.onRecord(record)
		}

		for i := range s.columns {
			s.row[i] = convertTSVValue(record[i], s.columnTypes[i])
		}
		s.inserted++
		s.batchInserted++

		if s.scanned%s.logEvery == 0 {
			s.logger.Printf("etl: %s scanned=%d inserted=%d skipped=%d elapsed=%s", s.table, s.scanned, s.inserted, s.skipped, time.Since(s.start).Truncate(time.Second))
		}

		return true
	}
}

func (s *tsvCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *tsvCopySource) Err() error {
	return s.err
}

func (s *tsvCopySource) Scanned() int {
	return s.scanned
}

func (s *tsvCopySource) Inserted() int {
	return s.inserted
}

func (s *tsvCopySource) Skipped() int {
	return s.skipped
}

func (s *tsvCopySource) beginBatch() {
	s.batchInserted = 0
}

func (s *tsvCopySource) Done() bool {
	return s.exhausted
}
