package search

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"

	"lumiere-api/internal/http/apiutil"
)

type SearchItem struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	StartYear     *int     `json:"startYear"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	AkaTitles     []string `json:"akaTitles"`
	Popularity    int      `json:"popularity"`
	Similarity    float64  `json:"similarity"`
}

type SearchResponse struct {
	Items []SearchItem `json:"items"`
}

type SuggestItem struct {
	Tconst        string  `json:"tconst"`
	TitleType     string  `json:"titleType"`
	StartYear     *int    `json:"startYear"`
	PrimaryTitle  string  `json:"primaryTitle"`
	OriginalTitle string  `json:"originalTitle"`
	Popularity    int     `json:"popularity"`
	Similarity    float64 `json:"similarity"`
}

type SuggestResponse struct {
	Items []SuggestItem `json:"items"`
}

const (
	searchTimeout         = 4 * time.Second
	suggestTimeout        = 120 * time.Millisecond
	maxSearchQueryLength  = 120
	maxSuggestQueryLength = 80
)

func NewSearchHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !enabled {
			return echo.NewHTTPError(http.StatusNotImplemented, "search is disabled")
		}

		query := normalizeSearchQuery(c.QueryParam("query"))
		if query == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "query is required")
		}
		if len(query) > maxSearchQueryLength {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("query must have at most %d characters", maxSearchQueryLength))
		}
		queryTokens := strings.Fields(query)

		typeGroup, err := apiutil.ParseTypeGroup(c.QueryParam("type"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		typeList := typeListForGroup(typeGroup)

		limit, err := apiutil.ParseClampedLimit(c.QueryParam("limit"), 20, 1, 50)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		enablePhrase := len(queryTokens) > 1
		ctx, cancel := context.WithTimeout(c.Request().Context(), searchTimeout)
		defer cancel()

		sql := buildSearchSQL(enablePhrase)
		results, err := runSearchQuery(ctx, pool, sql, query, typeList, limit)
		if err != nil {
			return searchQueryError(err, "search timed out")
		}

		return c.JSON(http.StatusOK, SearchResponse{
			Items: results,
		})
	}
}

func NewSuggestHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !enabled {
			return echo.NewHTTPError(http.StatusNotImplemented, "search is disabled")
		}

		query := normalizeSearchQuery(c.QueryParam("query"))
		if query == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "query is required")
		}
		if len(query) < 2 {
			return echo.NewHTTPError(http.StatusBadRequest, "query must have at least 2 characters")
		}
		if len(query) > maxSuggestQueryLength {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("query must have at most %d characters", maxSuggestQueryLength))
		}

		typeGroup, err := apiutil.ParseTypeGroup(c.QueryParam("type"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		typeList := typeListForGroup(typeGroup)

		limit, err := apiutil.ParseClampedLimit(c.QueryParam("limit"), 10, 1, 15)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		enablePhrasePrefix := len(strings.Fields(query)) > 1
		regexPattern := buildSuggestPrefixPattern(query)
		sql := buildSuggestSQL(enablePhrasePrefix)

		ctx, cancel := context.WithTimeout(c.Request().Context(), suggestTimeout)
		defer cancel()

		results, err := runSuggestQuery(ctx, pool, sql, query, typeList, regexPattern, limit)
		if err != nil {
			return searchQueryError(err, "search suggestion timed out")
		}

		return c.JSON(http.StatusOK, SuggestResponse{
			Items: results,
		})
	}
}

func buildSearchSQL(enablePhrase bool) string {
	// Conjunction match - all tokens must be present (any order)
	clauses := []string{
		"primary_title @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(3.0)",
		"original_title @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(2.0)",
		"aka_titles @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(0.5)",
	}

	if enablePhrase {
		// Phrase match with slop=3 - tokens in order, higher boost for accuracy
		// Use ICU tokenizer to convert query text to token array
		phraseTokens := "(($1::text)::pdb.icu('stopwords_language=english', 'ascii_folding=true')::text[])"
		clauses = append(clauses,
			fmt.Sprintf("CASE WHEN cardinality(%s) > 1 THEN primary_title @@@ pdb.phrase(%s, 3)::pdb.boost(6.0) ELSE FALSE END", phraseTokens, phraseTokens),
			fmt.Sprintf("CASE WHEN cardinality(%s) > 1 THEN original_title @@@ pdb.phrase(%s, 3)::pdb.boost(4.0) ELSE FALSE END", phraseTokens, phraseTokens),
		)
	}

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(`
SELECT
  tconst,
  title_type,
  start_year,
  primary_title,
  original_title,
  aka_titles,
  popularity,
  pdb.score(tconst) AS score
FROM title_search
WHERE title_type = ANY($2)
  AND $1::text <> ''
  AND (
  `)
	sqlBuilder.WriteString(strings.Join(clauses, "\n      OR "))
	sqlBuilder.WriteString(`
  )
ORDER BY
  popularity DESC,
  pdb.score(tconst) DESC,
  tconst DESC
LIMIT $3`)
	return sqlBuilder.String()
}

func runSearchQuery(
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	query string,
	typeList []string,
	limit int,
) ([]SearchItem, error) {
	rows, err := pool.Query(ctx, sql, query, typeList, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SearchItem, 0, limit)
	for rows.Next() {
		var (
			tconst        string
			resultType    string
			startYear     pgtype.Int4
			primaryTitle  string
			originalTitle string
			akaTitles     []string
			popularity    int
			score         float64
		)
		if err := rows.Scan(&tconst, &resultType, &startYear, &primaryTitle, &originalTitle, &akaTitles, &popularity, &score); err != nil {
			return nil, err
		}
		if akaTitles == nil {
			akaTitles = []string{}
		}
		results = append(results, SearchItem{
			Tconst:        tconst,
			TitleType:     resultType,
			StartYear:     intPtrFromPg(startYear),
			PrimaryTitle:  primaryTitle,
			OriginalTitle: originalTitle,
			AkaTitles:     akaTitles,
			Popularity:    popularity,
			Similarity:    score,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func intPtrFromPg(val pgtype.Int4) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int32)
	return &v
}

func buildSuggestSQL(enablePhrasePrefix bool) string {
	clauses := []string{
		"primary_title::pdb.alias('primary_title_exact') @@@ pdb.regex($3::text)::pdb.boost(12.0)",
		"original_title::pdb.alias('original_title_exact') @@@ pdb.regex($3::text)::pdb.boost(8.0)",
	}

	if enablePhrasePrefix {
		phraseTokens := "(($1::text)::pdb.icu('stopwords_language=english', 'ascii_folding=true')::text[])"
		clauses = append(clauses,
			fmt.Sprintf("CASE WHEN cardinality(%s) > 1 THEN primary_title @@@ pdb.phrase_prefix(%s, 32)::pdb.boost(5.0) ELSE FALSE END", phraseTokens, phraseTokens),
			fmt.Sprintf("CASE WHEN cardinality(%s) > 1 THEN original_title @@@ pdb.phrase_prefix(%s, 24)::pdb.boost(3.0) ELSE FALSE END", phraseTokens, phraseTokens),
		)
	}

	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(`
SELECT
  tconst,
  title_type,
  start_year,
  primary_title,
  original_title,
  popularity,
  pdb.score(tconst) AS score
FROM title_search
WHERE title_type = ANY($2)
  AND $1::text <> ''
  AND (
  `)
	sqlBuilder.WriteString(strings.Join(clauses, "\n      OR "))
	sqlBuilder.WriteString(`
  )
ORDER BY
  pdb.score(tconst) DESC,
  popularity DESC,
  tconst DESC
LIMIT $4`)
	return sqlBuilder.String()
}

func runSuggestQuery(
	ctx context.Context,
	pool *pgxpool.Pool,
	sql string,
	query string,
	typeList []string,
	regexPattern string,
	limit int,
) ([]SuggestItem, error) {
	rows, err := pool.Query(ctx, sql, query, typeList, regexPattern, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SuggestItem, 0, limit)
	for rows.Next() {
		var (
			tconst        string
			resultType    string
			startYear     pgtype.Int4
			primaryTitle  string
			originalTitle string
			popularity    int
			score         float64
		)
		if err := rows.Scan(&tconst, &resultType, &startYear, &primaryTitle, &originalTitle, &popularity, &score); err != nil {
			return nil, err
		}

		results = append(results, SuggestItem{
			Tconst:        tconst,
			TitleType:     resultType,
			StartYear:     intPtrFromPg(startYear),
			PrimaryTitle:  primaryTitle,
			OriginalTitle: originalTitle,
			Popularity:    popularity,
			Similarity:    score,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func buildSuggestPrefixPattern(query string) string {
	return regexp.QuoteMeta(query) + ".*"
}

func typeListForGroup(typeGroup string) []string {
	switch typeGroup {
	case apiutil.TypeGroupSeries:
		return []string{"tvseries", "tvminiseries", "tvspecial"}
	case apiutil.TypeGroupMovies:
		return []string{"movie", "tvmovie"}
	default:
		return nil
	}
}

func searchQueryError(err error, timeoutMessage string) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return echo.NewHTTPError(http.StatusGatewayTimeout, timeoutMessage)
	}
	return err
}

func normalizeSearchQuery(raw string) string {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return ""
	}
	return strings.ToLower(strings.Join(parts, " "))
}
