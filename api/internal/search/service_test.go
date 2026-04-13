package search

import (
	"context"
	"errors"
	"testing"
)

type stubRepository struct {
	searchItems   []SearchItem
	suggestItems  []SuggestItem
	err           error
	lastSearch    searchQuery
	lastSuggest   suggestQuery
	searchCalled  bool
	suggestCalled bool
}

func (r *stubRepository) Search(_ context.Context, query searchQuery) ([]SearchItem, error) {
	r.lastSearch = query
	r.searchCalled = true
	return r.searchItems, r.err
}

func (r *stubRepository) Suggest(_ context.Context, query suggestQuery) ([]SuggestItem, error) {
	r.lastSuggest = query
	r.suggestCalled = true
	return r.suggestItems, r.err
}

func TestSearchRejectsEmptyQuery(t *testing.T) {
	svc := NewService(&stubRepository{})

	_, err := svc.Search(context.Background(), SearchRequest{
		Query:     "   ",
		TypeGroup: "movies",
		Limit:     20,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %T", err)
	}
	if validationErr.Message != "query is required" {
		t.Fatalf("unexpected message %q", validationErr.Message)
	}
}

func TestSearchNormalizesAndDelegates(t *testing.T) {
	repo := &stubRepository{
		searchItems: []SearchItem{{Tconst: "tt1"}},
	}
	svc := NewService(repo)

	resp, err := svc.Search(context.Background(), SearchRequest{
		Query:     "  Dark   Knight  ",
		TypeGroup: "movies",
		Limit:     20,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if !repo.searchCalled {
		t.Fatal("expected search repository call")
	}
	if repo.lastSearch.Query != "dark knight" {
		t.Fatalf("unexpected normalized query %q", repo.lastSearch.Query)
	}
	if !repo.lastSearch.EnablePhrase {
		t.Fatal("expected phrase search to be enabled")
	}
	if len(repo.lastSearch.TypeList) != 2 {
		t.Fatalf("unexpected type list size %d", len(repo.lastSearch.TypeList))
	}
	if len(resp.Items) != 1 || resp.Items[0].Tconst != "tt1" {
		t.Fatal("unexpected response items")
	}
}

func TestSuggestRejectsShortQuery(t *testing.T) {
	svc := NewService(&stubRepository{})

	_, err := svc.Suggest(context.Background(), SuggestRequest{
		Query:     "a",
		TypeGroup: "movies",
		Limit:     10,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %T", err)
	}
	if validationErr.Message != "query must have at least 2 characters" {
		t.Fatalf("unexpected message %q", validationErr.Message)
	}
}

func TestSuggestBuildsPrefixPattern(t *testing.T) {
	repo := &stubRepository{
		suggestItems: []SuggestItem{{Tconst: "tt2"}},
	}
	svc := NewService(repo)

	resp, err := svc.Suggest(context.Background(), SuggestRequest{
		Query:     "Night",
		TypeGroup: "movies",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("suggest: %v", err)
	}
	if !repo.suggestCalled {
		t.Fatal("expected suggest repository call")
	}
	if repo.lastSuggest.Query != "night" {
		t.Fatalf("unexpected normalized query %q", repo.lastSuggest.Query)
	}
	if repo.lastSuggest.RegexPattern != "night.*" {
		t.Fatalf("unexpected regex pattern %q", repo.lastSuggest.RegexPattern)
	}
	if len(resp.Items) != 1 || resp.Items[0].Tconst != "tt2" {
		t.Fatal("unexpected response items")
	}
}
