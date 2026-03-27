package discover

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
)

type Item struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	StartYear     *int     `json:"startYear"`
	EndYear       *int     `json:"endYear"`
	Genres        []string `json:"genres"`
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Response struct {
	Items []Item `json:"items"`
	Meta  Meta   `json:"meta"`
}

type Meta struct {
	Sort           string  `json:"sort"`
	Limit          int     `json:"limit"`
	HasMore        bool    `json:"hasMore"`
	NextCursor     *string `json:"nextCursor,omitempty"`
	AppliedFilters Filter  `json:"appliedFilters"`
}

type Filter struct {
	Type      string   `json:"type"`
	Genres    []string `json:"genres"`
	YearFrom  *int     `json:"yearFrom,omitempty"`
	YearTo    *int     `json:"yearTo,omitempty"`
	MinVotes  *int     `json:"minVotes,omitempty"`
	MinRating *float64 `json:"minRating,omitempty"`
}

type discoverSort string

const (
	discoverSortPopular  discoverSort = "popular"
	discoverSortTopRated discoverSort = "top_rated"
	discoverSortNewest   discoverSort = "newest"
	discoverSortOldest   discoverSort = "oldest"
)

type discoverCursor struct {
	Sort        discoverSort `json:"sort"`
	Tconst      string       `json:"tconst"`
	VotesKey    *int         `json:"votesKey,omitempty"`
	YearKey     *int         `json:"yearKey,omitempty"`
	RatingKey   *float64     `json:"ratingKey,omitempty"`
	Fingerprint string       `json:"fingerprint"`
}

func NewHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		titleType := strings.ToLower(strings.TrimSpace(c.QueryParam("type")))
		yearFromRaw := strings.TrimSpace(c.QueryParam("year_from"))
		yearToRaw := strings.TrimSpace(c.QueryParam("year_to"))
		sortMode, err := parseDiscoverSort(c.QueryParam("sort"))
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "sort must be one of: popular, top_rated, newest, oldest"})
		}

		var typeGroup string
		switch titleType {
		case "series":
			typeGroup = "series"
		case "movies":
			typeGroup = "movies"
		default:
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "type must be 'series' or 'movies'"})
		}

		genres, err := parseDiscoverGenres(c)
		if err != nil {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}

		var yearFrom *int
		if yearFromRaw != "" {
			parsed, err := strconv.Atoi(yearFromRaw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid year_from"})
			}
			yearFrom = &parsed
		}

		var yearTo *int
		if yearToRaw != "" {
			parsed, err := strconv.Atoi(yearToRaw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid year_to"})
			}
			yearTo = &parsed
		}
		if yearFrom != nil && yearTo != nil && *yearFrom > *yearTo {
			return c.JSON(http.StatusBadRequest, map[string]string{"error": "year_from must be <= year_to"})
		}

		var minVotes *int
		if raw := strings.TrimSpace(c.QueryParam("min_votes")); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 0 {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid min_votes"})
			}
			minVotes = &parsed
		}

		var minRating *float64
		if raw := strings.TrimSpace(c.QueryParam("min_rating")); raw != "" {
			parsed, err := strconv.ParseFloat(raw, 64)
			if err != nil || parsed < 0 || parsed > 10 {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid min_rating"})
			}
			minRating = &parsed
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

		fingerprint := discoverFilterFingerprint(sortMode, typeGroup, genres, yearFrom, yearTo, minVotes, minRating)
		var cursor *discoverCursor
		if raw := strings.TrimSpace(c.QueryParam("cursor")); raw != "" {
			parsed, err := decodeDiscoverCursor(raw)
			if err != nil {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
			}
			if parsed.Sort != sortMode {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "cursor sort does not match requested sort"})
			}
			if parsed.Fingerprint != fingerprint {
				return c.JSON(http.StatusBadRequest, map[string]string{"error": "cursor does not match requested filters"})
			}
			cursor = &parsed
		}

		args := make([]any, 0, 16)
		args = append(args, typeGroup)
		param := 2
		var where strings.Builder
		where.WriteString("WHERE d.type_group = $1")
		switch sortMode {
		case discoverSortTopRated:
			where.WriteString(" AND d.average_rating IS NOT NULL AND d.num_votes IS NOT NULL")
		case discoverSortNewest, discoverSortOldest:
			where.WriteString(" AND d.start_year IS NOT NULL AND d.num_votes IS NOT NULL")
		default:
			where.WriteString(" AND d.num_votes IS NOT NULL")
		}

		if yearFrom != nil {
			where.WriteString(fmt.Sprintf(" AND d.start_year >= $%d", param))
			args = append(args, *yearFrom)
			param++
		}
		if yearTo != nil {
			where.WriteString(fmt.Sprintf(" AND d.start_year <= $%d", param))
			args = append(args, *yearTo)
			param++
		}
		if minVotes != nil {
			where.WriteString(fmt.Sprintf(" AND d.num_votes >= $%d", param))
			args = append(args, *minVotes)
			param++
		}
		if minRating != nil {
			where.WriteString(fmt.Sprintf(" AND d.average_rating >= $%d::numeric", param))
			args = append(args, *minRating)
			param++
		}
		for _, genre := range genres {
			where.WriteString(fmt.Sprintf(` AND EXISTS (
    SELECT 1
    FROM discover_genre dg
    WHERE dg.type_group = d.type_group
      AND dg.tconst = d.tconst
      AND dg.genre = $%d
)`, param))
			args = append(args, genre)
			param++
		}

		if cursor != nil {
			switch sortMode {
			case discoverSortTopRated:
				if cursor.RatingKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where.WriteString(fmt.Sprintf(" AND (d.average_rating, d.num_votes, d.tconst) < ($%d::numeric, $%d, $%d)", param, param+1, param+2))
				args = append(args, *cursor.RatingKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			case discoverSortNewest:
				if cursor.YearKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where.WriteString(fmt.Sprintf(" AND (d.start_year, d.num_votes, d.tconst) < ($%d, $%d, $%d)", param, param+1, param+2))
				args = append(args, *cursor.YearKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			case discoverSortOldest:
				if cursor.YearKey == nil || cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where.WriteString(fmt.Sprintf(" AND (d.start_year > $%d OR (d.start_year = $%d AND (d.num_votes, d.tconst) < ($%d, $%d)))", param, param, param+1, param+2))
				args = append(args, *cursor.YearKey, *cursor.VotesKey, cursor.Tconst)
				param += 3
			default:
				if cursor.VotesKey == nil {
					return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid cursor"})
				}
				where.WriteString(fmt.Sprintf(" AND (d.num_votes, d.tconst) < ($%d, $%d)", param, param+1))
				args = append(args, *cursor.VotesKey, cursor.Tconst)
				param += 2
			}
		}

		sqlLimit := limit + 1
		args = append(args, sqlLimit)
		orderClause := discoverOrderClause(sortMode)
		sql := fmt.Sprintf(`
SELECT d.tconst, d.title_type, d.primary_title, d.original_title, d.start_year, d.end_year,
       d.genres, d.average_rating::float8, d.num_votes
FROM discover_core d
%s
ORDER BY %s
LIMIT $%d`, where.String(), orderClause, param)

		ctx := c.Request().Context()
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		defer rows.Close()

		results := make([]Item, 0, sqlLimit)
		rowCursors := make([]discoverCursor, 0, sqlLimit)
		for rows.Next() {
			var (
				tconst        string
				ttype         string
				primaryTitle  string
				originalTitle string
				startYear     pgtype.Int4
				endYear       pgtype.Int4
				genresArr     []string
				avgRating     pgtype.Float8
				numVotes      pgtype.Int4
			)
			if err := rows.Scan(
				&tconst,
				&ttype,
				&primaryTitle,
				&originalTitle,
				&startYear,
				&endYear,
				&genresArr,
				&avgRating,
				&numVotes,
			); err != nil {
				return err
			}
			if genresArr == nil {
				genresArr = []string{}
			}
			item := Item{
				Tconst:        tconst,
				TitleType:     ttype,
				PrimaryTitle:  primaryTitle,
				OriginalTitle: originalTitle,
				StartYear:     intPtrFromPg(startYear),
				EndYear:       intPtrFromPg(endYear),
				Genres:        genresArr,
				AverageRating: floatPtrFromPg(avgRating),
				NumVotes:      intPtrFromPg(numVotes),
			}
			results = append(results, item)

			cur := discoverCursor{
				Sort:        sortMode,
				Tconst:      tconst,
				Fingerprint: fingerprint,
			}
			cur.VotesKey = intPtrFromPg(numVotes)
			if cur.VotesKey == nil {
				return errors.New("discover cursor invariant: missing votes key")
			}
			switch sortMode {
			case discoverSortTopRated:
				cur.RatingKey = floatPtrFromPg(avgRating)
				if cur.RatingKey == nil {
					return errors.New("discover cursor invariant: missing rating key")
				}
			case discoverSortNewest:
				cur.YearKey = intPtrFromPg(startYear)
				if cur.YearKey == nil {
					return errors.New("discover cursor invariant: missing year key")
				}
			case discoverSortOldest:
				cur.YearKey = intPtrFromPg(startYear)
				if cur.YearKey == nil {
					return errors.New("discover cursor invariant: missing year key")
				}
			}
			rowCursors = append(rowCursors, cur)
		}
		if err := rows.Err(); err != nil {
			return err
		}

		hasMore := len(results) > limit
		if hasMore {
			results = results[:limit]
			rowCursors = rowCursors[:limit]
		}

		var nextCursor *string
		if hasMore && len(rowCursors) > 0 {
			encoded, err := encodeDiscoverCursor(rowCursors[len(rowCursors)-1])
			if err != nil {
				return err
			}
			nextCursor = &encoded
		}

		resp := Response{
			Items: results,
			Meta: Meta{
				Sort:       string(sortMode),
				Limit:      limit,
				HasMore:    hasMore,
				NextCursor: nextCursor,
				AppliedFilters: Filter{
					Type:      titleType,
					Genres:    genres,
					YearFrom:  yearFrom,
					YearTo:    yearTo,
					MinVotes:  minVotes,
					MinRating: minRating,
				},
			},
		}

		return c.JSON(http.StatusOK, resp)
	}
}

func parseDiscoverSort(raw string) (discoverSort, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "popular":
		return discoverSortPopular, nil
	case "top_rated":
		return discoverSortTopRated, nil
	case "newest":
		return discoverSortNewest, nil
	case "oldest":
		return discoverSortOldest, nil
	default:
		return "", errors.New("invalid sort")
	}
}

func parseDiscoverGenres(c *echo.Context) ([]string, error) {
	rawValues := append([]string{}, c.QueryParams()["genres"]...)
	rawValues = append(rawValues, c.QueryParams()["genre"]...)

	seen := map[string]struct{}{}
	genres := make([]string, 0, 3)
	for _, raw := range rawValues {
		for part := range strings.SplitSeq(raw, ",") {
			g := strings.ToLower(strings.TrimSpace(part))
			if g == "" {
				continue
			}
			if _, ok := seen[g]; ok {
				continue
			}
			seen[g] = struct{}{}
			genres = append(genres, g)
		}
	}
	if len(genres) > 3 {
		return nil, errors.New("max 3 genres are allowed")
	}
	sort.Strings(genres)
	return genres, nil
}

func encodeDiscoverCursor(cur discoverCursor) (string, error) {
	data, err := json.Marshal(cur)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeDiscoverCursor(raw string) (discoverCursor, error) {
	var cur discoverCursor
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cur, err
	}
	if err := json.Unmarshal(data, &cur); err != nil {
		return cur, err
	}
	if cur.Tconst == "" || cur.Sort == "" || cur.Fingerprint == "" {
		return cur, errors.New("invalid cursor payload")
	}
	return cur, nil
}

func discoverFilterFingerprint(
	sortMode discoverSort,
	typeGroup string,
	genres []string,
	yearFrom *int,
	yearTo *int,
	minVotes *int,
	minRating *float64,
) string {
	yearFromToken := ""
	if yearFrom != nil {
		yearFromToken = strconv.Itoa(*yearFrom)
	}
	yearToToken := ""
	if yearTo != nil {
		yearToToken = strconv.Itoa(*yearTo)
	}
	minVotesToken := ""
	if minVotes != nil {
		minVotesToken = strconv.Itoa(*minVotes)
	}
	minRatingToken := ""
	if minRating != nil {
		minRatingToken = strconv.FormatFloat(*minRating, 'f', 4, 64)
	}
	return strings.Join([]string{
		string(sortMode),
		typeGroup,
		strings.Join(genres, ","),
		yearFromToken,
		yearToToken,
		minVotesToken,
		minRatingToken,
	}, "|")
}

func discoverOrderClause(sortMode discoverSort) string {
	switch sortMode {
	case discoverSortTopRated:
		return "d.average_rating DESC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	case discoverSortNewest:
		return "d.start_year DESC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	case discoverSortOldest:
		return "d.start_year ASC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	default:
		return "d.num_votes DESC NULLS LAST, d.tconst DESC"
	}
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
