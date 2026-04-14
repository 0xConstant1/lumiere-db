package etl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func downloadDatasets(ctx context.Context, cfg Config, logger *log.Logger) error {
	baseURL := strings.TrimRight(cfg.BaseURL, "/")
	workerCount := min(clampPositive(cfg.DownloadConcurrency, 3), len(imdbDatasetFiles))

	jobs := make(chan string, len(imdbDatasetFiles))
	for _, file := range imdbDatasetFiles {
		jobs <- file
	}
	close(jobs)

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	for range workerCount {
		wg.Go(func() {
			for file := range jobs {
				if workerCtx.Err() != nil {
					return
				}
				logger.Printf("etl: download %s", file)
				if err := downloadFile(workerCtx, baseURL, cfg.DataDir, file, cfg.ForceDownload, logger); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
			}
		})
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}
	if workerCtx.Err() != nil && !errors.Is(workerCtx.Err(), context.Canceled) {
		return workerCtx.Err()
	}
	return nil
}

func downloadFile(ctx context.Context, baseURL, dataDir, filename string, force bool, logger *log.Logger) error {
	const downloadRequestTimeout = 2 * time.Hour

	dest := filepath.Join(dataDir, filename)
	if !force {
		if info, err := os.Stat(dest); err == nil && info.Size() > 0 {
			logger.Printf("etl: skip %s (already downloaded)", filename)
			return nil
		}
	}

	url := baseURL + "/" + filename
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	client := &http.Client{Timeout: downloadRequestTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: status %s", filename, resp.Status)
	}

	tmp := dest + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}
	cleanupTmp := true
	defer func() {
		if cleanupTmp {
			_ = os.Remove(tmp)
		}
	}()

	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if err := out.Close(); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}

	if err := os.Remove(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("download %s: remove existing destination: %w", filename, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		return fmt.Errorf("download %s: %w", filename, err)
	}
	cleanupTmp = false

	return nil
}
