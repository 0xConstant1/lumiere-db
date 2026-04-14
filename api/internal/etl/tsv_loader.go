package etl

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/gzip"
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
