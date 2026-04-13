package apiutil

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	TypeGroupSeries = "series"
	TypeGroupMovies = "movies"
)

func ParseTypeGroup(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case TypeGroupSeries:
		return TypeGroupSeries, nil
	case TypeGroupMovies:
		return TypeGroupMovies, nil
	default:
		return "", fmt.Errorf("type must be 'series' or 'movies'")
	}
}

func ParseClampedLimit(raw string, def int, min int, max int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return def, nil
	}

	limit, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("limit must be an integer")
	}
	if limit < min {
		return min, nil
	}
	if limit > max {
		return max, nil
	}
	return limit, nil
}
