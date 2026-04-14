package discover

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"

	discovercore "lumiere-api/internal/discover"
	"lumiere-api/internal/http/apiutil"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
)

func NewHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	svc := discovercore.NewService(discovercore.NewPGRepository(pool))

	return func(c *echo.Context) error {
		typeGroup, err := apiutil.ParseTypeGroup(c.QueryParam("type"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		sortMode, err := parseDiscoverSort(c.QueryParam("sort"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, "sort must be one of: popular, top_rated, newest, oldest")
		}

		genres, err := parseDiscoverGenres(c)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		yearFrom, err := parseOptionalYear(c.QueryParam("year_from"), "invalid year_from")
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		yearTo, err := parseOptionalYear(c.QueryParam("year_to"), "invalid year_to")
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		if yearFrom != nil && yearTo != nil && *yearFrom > *yearTo {
			return echo.NewHTTPError(http.StatusBadRequest, "year_from must be <= year_to")
		}

		minVotes, err := parseOptionalNonNegativeInt(c.QueryParam("min_votes"), "invalid min_votes")
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		minRating, err := parseOptionalRating(c.QueryParam("min_rating"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		limit, err := apiutil.ParseClampedLimit(c.QueryParam("limit"), 20, 1, 50)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		resp, err := svc.Discover(c.Request().Context(), discovercore.Request{
			TypeGroup: typeGroup,
			Genres:    genres,
			YearFrom:  yearFrom,
			YearTo:    yearTo,
			MinVotes:  minVotes,
			MinRating: minRating,
			Sort:      sortMode,
			Limit:     limit,
			Cursor:    c.QueryParam("cursor"),
		})
		if err != nil {
			var validationErr *discovercore.ValidationError
			if errors.As(err, &validationErr) {
				return echo.NewHTTPError(http.StatusBadRequest, validationErr.Message)
			}
			return err
		}

		return c.JSON(http.StatusOK, resp)
	}
}

func parseDiscoverSort(raw string) (discovercore.Sort, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(discovercore.SortPopular):
		return discovercore.SortPopular, nil
	case string(discovercore.SortTopRated):
		return discovercore.SortTopRated, nil
	case string(discovercore.SortNewest):
		return discovercore.SortNewest, nil
	case string(discovercore.SortOldest):
		return discovercore.SortOldest, nil
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

func parseOptionalYear(raw string, message string) (*int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return nil, errors.New(message)
	}
	return &parsed, nil
}

func parseOptionalNonNegativeInt(raw string, message string) (*int, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed < 0 {
		return nil, errors.New(message)
	}
	return &parsed, nil
}

func parseOptionalRating(raw string) (*float64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || parsed < 0 || parsed > 10 {
		return nil, errors.New("invalid min_rating")
	}
	return &parsed, nil
}
