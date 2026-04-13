package search

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"lumiere-api/internal/http/apiutil"
)

const (
	searchTimeout         = 4 * time.Second
	suggestTimeout        = 120 * time.Millisecond
	maxSearchQueryLength  = 120
	maxSuggestQueryLength = 80
)

type Repository interface {
	Search(ctx context.Context, query searchQuery) ([]SearchItem, error)
	Suggest(ctx context.Context, query suggestQuery) ([]SuggestItem, error)
}

type Service struct {
	repo Repository
}

type ValidationError struct {
	Message string
}

func (e *ValidationError) Error() string {
	return e.Message
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Search(ctx context.Context, req SearchRequest) (SearchResponse, error) {
	query := normalizeSearchQuery(req.Query)
	if query == "" {
		return SearchResponse{}, &ValidationError{Message: "query is required"}
	}
	if len(query) > maxSearchQueryLength {
		return SearchResponse{}, &ValidationError{Message: fmt.Sprintf("query must have at most %d characters", maxSearchQueryLength)}
	}

	typeList := typeListForGroup(req.TypeGroup)
	if len(typeList) == 0 {
		return SearchResponse{}, &ValidationError{Message: "type must be 'series' or 'movies'"}
	}

	searchCtx, cancel := context.WithTimeout(ctx, searchTimeout)
	defer cancel()

	results, err := s.repo.Search(searchCtx, searchQuery{
		Query:        query,
		TypeList:     typeList,
		Limit:        req.Limit,
		EnablePhrase: len(strings.Fields(query)) > 1,
	})
	if err != nil {
		return SearchResponse{}, err
	}

	return SearchResponse{Items: results}, nil
}

func (s *Service) Suggest(ctx context.Context, req SuggestRequest) (SuggestResponse, error) {
	query := normalizeSearchQuery(req.Query)
	if query == "" {
		return SuggestResponse{}, &ValidationError{Message: "query is required"}
	}
	if len(query) < 2 {
		return SuggestResponse{}, &ValidationError{Message: "query must have at least 2 characters"}
	}
	if len(query) > maxSuggestQueryLength {
		return SuggestResponse{}, &ValidationError{Message: fmt.Sprintf("query must have at most %d characters", maxSuggestQueryLength)}
	}

	typeList := typeListForGroup(req.TypeGroup)
	if len(typeList) == 0 {
		return SuggestResponse{}, &ValidationError{Message: "type must be 'series' or 'movies'"}
	}

	suggestCtx, cancel := context.WithTimeout(ctx, suggestTimeout)
	defer cancel()

	results, err := s.repo.Suggest(suggestCtx, suggestQuery{
		Query:              query,
		TypeList:           typeList,
		RegexPattern:       buildSuggestPrefixPattern(query),
		Limit:              req.Limit,
		EnablePhrasePrefix: len(strings.Fields(query)) > 1,
	})
	if err != nil {
		return SuggestResponse{}, err
	}

	return SuggestResponse{Items: results}, nil
}

func IsValidationError(err error) bool {
	var validationErr *ValidationError
	return errors.As(err, &validationErr)
}

func typeListForGroup(typeGroup string) []string {
	switch typeGroup {
	case apiutil.TypeGroupSeries:
		return []string{"tvseries", "tvminiseries", "tvspecial"}
	case apiutil.TypeGroupMovies:
		return []string{"movie", "tvmovie"}
	default:
		return nil
	}
}

func buildSuggestPrefixPattern(query string) string {
	return regexp.QuoteMeta(query) + ".*"
}

func normalizeSearchQuery(raw string) string {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return ""
	}
	return strings.ToLower(strings.Join(parts, " "))
}
