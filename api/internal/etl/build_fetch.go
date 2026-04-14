package etl

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func fetchBasics(ctx context.Context, q querier, tconsts []string) (map[string]TitleBasics, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
FROM stg_title_basics
	WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, fmt.Errorf("fetch basics query: %w", err)
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
			return nil, fmt.Errorf("fetch basics scan: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch basics rows: %w", err)
	}
	return out, nil
}

func fetchRatings(ctx context.Context, q querier, tconsts []string) (map[string]Rating, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, averageRating, numVotes
FROM stg_title_ratings
	WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, fmt.Errorf("fetch ratings query: %w", err)
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
			return nil, fmt.Errorf("fetch ratings scan: %w", err)
		}
		out[tconst] = Rating{
			AverageRating: floatPtrFromPg(avg),
			NumVotes:      intPtrFromPg(votes),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch ratings rows: %w", err)
	}
	return out, nil
}

func fetchAkas(ctx context.Context, q querier, tconsts []string) (map[string]map[string][]Aka, error) {
	rows, err := q.Query(ctx, `
SELECT titleId, ordering, title, region, language, types, attributes, isOriginalTitle
FROM stg_title_akas
WHERE titleId = ANY($1)
	ORDER BY titleId, ordering`, tconsts)
	if err != nil {
		return nil, fmt.Errorf("fetch akas query: %w", err)
	}
	defer rows.Close()

	out := make(map[string]map[string][]Aka)
	for rows.Next() {
		var (
			titleID         string
			ordering        pgtype.Int4
			title           string
			region          string
			language        string
			types           string
			attributes      string
			isOriginalTitle pgtype.Bool
		)
		if err := rows.Scan(&titleID, &ordering, &title, &region, &language, &types, &attributes, &isOriginalTitle); err != nil {
			return nil, fmt.Errorf("fetch akas scan: %w", err)
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

		if _, ok := out[titleID]; !ok {
			out[titleID] = make(map[string][]Aka)
		}
		out[titleID][region] = append(out[titleID][region], aka)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch akas rows: %w", err)
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

func fetchPrincipals(ctx context.Context, q querier, tconsts []string, maxActors int, maxProducers int) ([]principalRow, error) {
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
		return nil, fmt.Errorf("fetch principals query: %w", err)
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
			return nil, fmt.Errorf("fetch principals scan: %w", err)
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch principals rows: %w", err)
	}
	return out, nil
}

func fetchCrew(ctx context.Context, q querier, tconsts []string) (map[string]crewLists, error) {
	rows, err := q.Query(ctx, `
SELECT tconst, directors, writers
FROM stg_title_crew
	WHERE tconst = ANY($1)`, tconsts)
	if err != nil {
		return nil, fmt.Errorf("fetch crew query: %w", err)
	}
	defer rows.Close()

	out := make(map[string]crewLists)
	for rows.Next() {
		var tconst, directors, writers string
		if err := rows.Scan(&tconst, &directors, &writers); err != nil {
			return nil, fmt.Errorf("fetch crew scan: %w", err)
		}
		out[tconst] = crewLists{
			Directors: splitList(directors),
			Writers:   splitList(writers),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch crew rows: %w", err)
	}
	return out, nil
}

func fetchNames(ctx context.Context, q querier, nconsts []string) (map[string]string, error) {
	if len(nconsts) == 0 {
		return map[string]string{}, nil
	}

	rows, err := q.Query(ctx, `
SELECT nconst, primaryName
FROM stg_name_basics
	WHERE nconst = ANY($1)`, nconsts)
	if err != nil {
		return nil, fmt.Errorf("fetch names query: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(nconsts))
	for rows.Next() {
		var nconst, name string
		if err := rows.Scan(&nconst, &name); err != nil {
			return nil, fmt.Errorf("fetch names scan: %w", err)
		}
		out[nconst] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fetch names rows: %w", err)
	}
	return out, nil
}

func fetchEpisodes(ctx context.Context, q querier, tconsts []string) (map[string][]Season, error) {
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
		return nil, fmt.Errorf("fetch episodes query: %w", err)
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
			return nil, fmt.Errorf("fetch episodes scan: %w", err)
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
		return nil, fmt.Errorf("fetch episodes rows: %w", err)
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
