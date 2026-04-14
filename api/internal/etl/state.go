package etl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func loadSourceState(ctx context.Context, pool *pgxpool.Pool) (map[string]imdbFileState, error) {
	return loadManifestState(ctx, pool, sourceStateTable)
}

func loadPendingSourceState(ctx context.Context, pool *pgxpool.Pool) (map[string]imdbFileState, error) {
	return loadManifestState(ctx, pool, pendingStateTable)
}

func loadManifestState(ctx context.Context, pool *pgxpool.Pool, tableName string) (map[string]imdbFileState, error) {
	exists, err := tableExists(ctx, pool, "public."+tableName)
	if err != nil {
		return nil, err
	}
	if !exists {
		return map[string]imdbFileState{}, nil
	}

	rows, err := pool.Query(ctx, fmt.Sprintf(`
SELECT file_name, last_modified, content_length
FROM %s`, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	state := make(map[string]imdbFileState)
	for rows.Next() {
		var (
			fileName      string
			lastModified  time.Time
			contentLength int64
		)
		if err := rows.Scan(&fileName, &lastModified, &contentLength); err != nil {
			return nil, err
		}
		state[fileName] = imdbFileState{
			FileName:      fileName,
			LastModified:  lastModified.UTC(),
			ContentLength: contentLength,
		}
	}
	return state, rows.Err()
}

func saveSourceState(ctx context.Context, pool *pgxpool.Pool, state map[string]imdbFileState) error {
	return saveManifestState(ctx, pool, sourceStateTable, state)
}

func savePendingSourceState(ctx context.Context, pool *pgxpool.Pool, state map[string]imdbFileState) error {
	return saveManifestState(ctx, pool, pendingStateTable, state)
}

func saveManifestState(ctx context.Context, pool *pgxpool.Pool, tableName string, state map[string]imdbFileState) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	for _, file := range imdbDatasetFiles {
		entry, ok := state[file]
		if !ok {
			return fmt.Errorf("missing source state for %s", file)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (file_name, last_modified, content_length, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (file_name) DO UPDATE
SET last_modified = EXCLUDED.last_modified,
    content_length = EXCLUDED.content_length,
    updated_at = now()`, tableName),
			entry.FileName,
			entry.LastModified,
			entry.ContentLength,
		); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE NOT (file_name = ANY($1))`, tableName), imdbDatasetFiles); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func clearPendingSourceState(ctx context.Context, pool *pgxpool.Pool) error {
	exists, err := tableExists(ctx, pool, "public."+pendingStateTable)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	_, err = pool.Exec(ctx, `DELETE FROM etl_pending_source_state`)
	return err
}

func acquireETLSchedulerLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, bool, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, false, err
	}

	var locked bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", etlSchedulerLockKey).Scan(&locked); err != nil {
		conn.Release()
		return nil, false, err
	}

	if !locked {
		conn.Release()
		return nil, false, nil
	}
	return conn, true, nil
}

func releaseETLSchedulerLock(conn *pgxpool.Conn) {
	if conn == nil {
		return
	}
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", etlSchedulerLockKey)
	conn.Release()
}
