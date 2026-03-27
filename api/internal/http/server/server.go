package server

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	discoverapi "lumiere-api/internal/http/discover"
	searchapi "lumiere-api/internal/http/search"
	titleapi "lumiere-api/internal/http/title"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
)

type errorResponse struct {
	Error     string `json:"error"`
	RequestID string `json:"requestId,omitempty"`
}

func Start(
	ctx context.Context,
	pool *pgxpool.Pool,
	port string,
	enablePGSearch bool,
	corsAllowOrigins []string,
	logger *log.Logger,
) error {
	const (
		readinessTimeout = 1 * time.Second
		shutdownTimeout  = 10 * time.Second
	)

	e := echo.New()
	e.IPExtractor = echo.ExtractIPDirect()
	e.HTTPErrorHandler = newHTTPErrorHandler(logger)
	e.Use(middleware.RequestID())
	e.Use(middleware.RequestLogger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: corsAllowOrigins,
		AllowMethods: []string{http.MethodGet, http.MethodOptions},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderXRequestID},
	}))

	e.GET("/health", func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})
	e.GET("/readyz", func(c *echo.Context) error {
		pingCtx, cancel := context.WithTimeout(c.Request().Context(), readinessTimeout)
		defer cancel()
		if err := pool.Ping(pingCtx); err != nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{
				"status": "not_ready",
				"error":  "database unavailable",
			})
		}
		return c.JSON(http.StatusOK, map[string]string{"status": "ready"})
	})

	e.GET("/titles/:tconst", titleapi.NewHandler(pool))
	e.GET("/search", searchapi.NewSearchHandler(pool, enablePGSearch))
	e.GET("/search/suggest", searchapi.NewSuggestHandler(pool, enablePGSearch))
	e.GET("/discover", discoverapi.NewHandler(pool))

	addr := ":" + port
	sc := echo.StartConfig{
		Address:         addr,
		HideBanner:      true,
		GracefulTimeout: shutdownTimeout,
		OnShutdownError: func(err error) {
			logger.Printf("api: shutdown error: %v", err)
		},
		BeforeServeFunc: func(s *http.Server) error {
			s.ReadHeaderTimeout = 5 * time.Second
			s.ReadTimeout = 15 * time.Second
			s.WriteTimeout = 30 * time.Second
			s.IdleTimeout = 60 * time.Second
			return nil
		},
	}

	logger.Printf("api: listening on %s", addr)
	if err := sc.Start(ctx, e); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func newHTTPErrorHandler(logger *log.Logger) echo.HTTPErrorHandler {
	return func(c *echo.Context, err error) {
		if r, _ := echo.UnwrapResponse(c.Response()); r != nil && r.Committed {
			return
		}

		status := http.StatusInternalServerError
		var sc echo.HTTPStatusCoder
		if errors.As(err, &sc) {
			if code := sc.StatusCode(); code != 0 {
				status = code
			}
		}

		requestID := requestIDFromContext(c)
		message := errorMessage(status, err)
		logger.Printf(
			"api: request error method=%s path=%s status=%d request_id=%s err=%v",
			c.Request().Method,
			c.Request().URL.Path,
			status,
			requestID,
			err,
		)

		if c.Request().Method == http.MethodHead {
			if cErr := c.NoContent(status); cErr != nil {
				logger.Printf("api: failed to send HEAD error response: %v", cErr)
			}
			return
		}

		if cErr := c.JSON(status, errorResponse{
			Error:     message,
			RequestID: requestID,
		}); cErr != nil {
			logger.Printf("api: failed to send error response: %v", cErr)
		}
	}
}

func requestIDFromContext(c *echo.Context) string {
	requestID := c.Response().Header().Get(echo.HeaderXRequestID)
	if requestID == "" {
		requestID = c.Request().Header.Get(echo.HeaderXRequestID)
	}
	return requestID
}

func errorMessage(status int, err error) string {
	if status >= http.StatusInternalServerError {
		return "internal server error"
	}

	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) {
		trimmed := strings.TrimSpace(httpErr.Message)
		if trimmed != "" {
			return trimmed
		}
	}

	if text := strings.TrimSpace(http.StatusText(status)); text != "" {
		return text
	}
	return "request failed"
}
