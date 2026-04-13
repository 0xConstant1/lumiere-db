package title

import (
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
)

func NewHandler(pool *pgxpool.Pool) echo.HandlerFunc {
	return func(c *echo.Context) error {
		tconst := c.Param("tconst")
		if tconst == "" {
			return echo.NewHTTPError(http.StatusBadRequest, "missing tconst")
		}

		ctx := c.Request().Context()
		var data []byte
		err := pool.QueryRow(ctx, `SELECT data FROM titles WHERE tconst = $1`, tconst).Scan(&data)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return echo.NewHTTPError(http.StatusNotFound, "title not found")
			}
			return err
		}

		return c.Blob(http.StatusOK, "application/json", data)
	}
}
