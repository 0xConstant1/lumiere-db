package db

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func RedactURL(raw string) string {
	schemeIdx := strings.Index(raw, "://")
	if schemeIdx == -1 {
		return raw
	}
	at := strings.LastIndex(raw, "@")
	if at == -1 {
		return raw
	}
	creds := raw[schemeIdx+3 : at]
	parts := strings.SplitN(creds, ":", 2)
	if len(parts) != 2 {
		return raw
	}
	return raw[:schemeIdx+3] + parts[0] + ":<redacted>@" + raw[at+1:]
}

func ConnectWithRetry(ctx context.Context, url string, logger *log.Logger) (*pgxpool.Pool, error) {
	const (
		maxAttempts = 10
		backoff     = 5 * time.Second
		pingTimeout = 3 * time.Second
	)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("db: context canceled while waiting for readiness: %w (last error: %v)", err, lastErr)
			}
			return nil, fmt.Errorf("db: context canceled while waiting for readiness: %w", err)
		}

		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
			pingErr := pool.Ping(pingCtx)
			cancel()
			if pingErr == nil {
				return pool, nil
			}
			pool.Close()
			lastErr = pingErr
		} else {
			lastErr = err
		}
		logger.Printf("db: not ready (attempt %d/%d): %v", attempt, maxAttempts, lastErr)

		if attempt == maxAttempts {
			break
		}

		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			if lastErr != nil {
				return nil, fmt.Errorf("db: context canceled during retry backoff: %w (last error: %v)", ctx.Err(), lastErr)
			}
			return nil, fmt.Errorf("db: context canceled during retry backoff: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("db: not ready after retries: %w", lastErr)
}
