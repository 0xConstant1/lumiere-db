package search

import (
	"context"
	"errors"
	"net/http"

	"lumiere-api/internal/http/apiutil"
	searchcore "lumiere-api/internal/search"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
)

func NewSearchHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	svc := searchcore.NewService(searchcore.NewPGRepository(pool))

	return func(c *echo.Context) error {
		if !enabled {
			return echo.NewHTTPError(http.StatusNotImplemented, "search is disabled")
		}

		typeGroup, err := apiutil.ParseTypeGroup(c.QueryParam("type"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		limit, err := apiutil.ParseClampedLimit(c.QueryParam("limit"), 20, 1, 50)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		resp, err := svc.Search(c.Request().Context(), searchcore.SearchRequest{
			Query:     c.QueryParam("query"),
			TypeGroup: typeGroup,
			Limit:     limit,
		})
		if err != nil {
			return mapSearchError(err, "search timed out")
		}

		return c.JSON(http.StatusOK, resp)
	}
}

func NewSuggestHandler(pool *pgxpool.Pool, enabled bool) echo.HandlerFunc {
	svc := searchcore.NewService(searchcore.NewPGRepository(pool))

	return func(c *echo.Context) error {
		if !enabled {
			return echo.NewHTTPError(http.StatusNotImplemented, "search is disabled")
		}

		typeGroup, err := apiutil.ParseTypeGroup(c.QueryParam("type"))
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		limit, err := apiutil.ParseClampedLimit(c.QueryParam("limit"), 10, 1, 15)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		resp, err := svc.Suggest(c.Request().Context(), searchcore.SuggestRequest{
			Query:     c.QueryParam("query"),
			TypeGroup: typeGroup,
			Limit:     limit,
		})
		if err != nil {
			return mapSearchError(err, "search suggestion timed out")
		}

		return c.JSON(http.StatusOK, resp)
	}
}

func mapSearchError(err error, timeoutMessage string) error {
	var validationErr *searchcore.ValidationError
	if errors.As(err, &validationErr) {
		return echo.NewHTTPError(http.StatusBadRequest, validationErr.Message)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return echo.NewHTTPError(http.StatusGatewayTimeout, timeoutMessage)
	}
	return err
}
