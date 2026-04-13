package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGRepository struct {
	pool *pgxpool.Pool
}

func NewPGRepository(pool *pgxpool.Pool) *PGRepository {
	return &PGRepository{pool: pool}
}

func (r *PGRepository) Search(ctx context.Context, query searchQuery) ([]SearchItem, error) {
	sql := buildSearchSQL(query.EnablePhrase)
	rows, err := r.pool.Query(ctx, sql, query.Query, query.TypeList, query.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SearchItem, 0, query.Limit)
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

func (r *PGRepository) Suggest(ctx context.Context, query suggestQuery) ([]SuggestItem, error) {
	sql := buildSuggestSQL(query.EnablePhrasePrefix)
	rows, err := r.pool.Query(ctx, sql, query.Query, query.TypeList, query.RegexPattern, query.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]SuggestItem, 0, query.Limit)
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

func buildSearchSQL(enablePhrase bool) string {
	clauses := []string{
		"primary_title @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(3.0)",
		"original_title @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(2.0)",
		"aka_titles @@@ pdb.match($1::text, conjunction_mode => true)::pdb.boost(0.5)",
	}

	if enablePhrase {
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

func intPtrFromPg(val pgtype.Int4) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int32)
	return &v
}
