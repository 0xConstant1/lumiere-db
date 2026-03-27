package search

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
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

func NewSearchHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !enabled {
			return c.JSON(http.StatusNotImplemented, map[string]string{"error": "search is disabled"})
		}

		query := normalizeSearchQuery(c.QueryParam("query"))
		if query == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "query is required"})
		}
		queryTokens := strings.Fields(query)

		titleType := strings.ToLower(strings.TrimSpace(c.QueryParam("type")))
		var typeList []string
		switch titleType {
		case "series":
			typeList = []string{"tvseries", "tvminiseries", "tvspecial"}
		case "movies":
			typeList = []string{"movie", "tvmovie"}
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "type must be 'series' or 'movies'"})
		}

		limit := 20
		if raw := c.QueryParam("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 50 {
			limit = 50
		}

		enablePhrase := len(queryTokens) > 1
		ctx := c.Request().Context()
		sql := buildSearchSQL(enablePhrase)
		results, err := runSearchQuery(ctx, pool, sql, query, typeList, limit)
		if err != nil {
			return err
		}

		return c.JSON(http.StatusOK, SearchResponse{
			Items: results,
		})
	}
}

func NewSuggestHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !enabled {
			return c.JSON(http.StatusNotImplemented, map[string]string{"error": "search is disabled"})
		}

		query := normalizeSearchQuery(c.QueryParam("query"))
		if query == "" {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "query is required"})
		}
		if len(query) < 2 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "query must have at least 2 characters"})
		}
		if len(query) > 80 {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "query must have at most 80 characters"})
		}

		titleType := strings.ToLower(strings.TrimSpace(c.QueryParam("type")))
		var typeList []string
		switch titleType {
		case "series":
			typeList = []string{"tvseries", "tvminiseries", "tvspecial"}
		case "movies":
			typeList = []string{"movie", "tvmovie"}
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "type must be 'series' or 'movies'"})
		}

		limit := 10
		if raw := c.QueryParam("limit"); raw != "" {
			if parsed, err := strconv.Atoi(raw); err == nil {
				limit = parsed
			}
		}
		if limit < 1 {
			limit = 1
		}
		if limit > 15 {
			limit = 15
		}

		enablePhrasePrefix := len(strings.Fields(query)) > 1
		regexPattern := buildSuggestPrefixPattern(query)
		sql := buildSuggestSQL(enablePhrasePrefix)

		ctx, cancel := context.WithTimeout(c.Request().Context(), 120*time.Millisecond)
		defer cancel()

		results, err := runSuggestQuery(ctx, pool, sql, query, typeList, regexPattern, limit)
		if err != nil {
			return err
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

func normalizeSearchQuery(raw string) string {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return ""
	}
	return strings.ToLower(strings.Join(parts, " "))
}
