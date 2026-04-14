package discover

import (
	"context"
	"errors"
	"testing"
)

type stubRepository struct {
	rows        []row
	err         error
	lastQuery   query
	calledQuery bool
}

func (r *stubRepository) List(_ context.Context, q query) ([]row, error) {
	r.lastQuery = q
	r.calledQuery = true
	return r.rows, r.err
}

func TestServiceDiscoverRejectsInvalidCursor(t *testing.T) {
	svc := NewService(&stubRepository{})

	_, err := svc.Discover(context.Background(), Request{
		TypeGroup: "movies",
		Sort:      SortPopular,
		Limit:     20,
		Cursor:    "not-base64",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %T", err)
	}
	if validationErr.Message != "invalid cursor" {
		t.Fatalf("unexpected message %q", validationErr.Message)
	}
}

func TestServiceDiscoverRejectsCursorSortMismatch(t *testing.T) {
	encoded, err := encodeCursor(cursor{
		Sort:        SortNewest,
		Tconst:      "tt1",
		VotesKey:    intPtr(10),
		YearKey:     intPtr(2024),
		Fingerprint: "popular|movies||||",
	})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	svc := NewService(&stubRepository{})
	_, err = svc.Discover(context.Background(), Request{
		TypeGroup: "movies",
		Sort:      SortPopular,
		Limit:     20,
		Cursor:    encoded,
	})
	if err == nil {
		t.Fatal("expected error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %T", err)
	}
	if validationErr.Message != "cursor sort does not match requested sort" {
		t.Fatalf("unexpected message %q", validationErr.Message)
	}
}

func TestServiceDiscoverBuildsResponseAndNextCursor(t *testing.T) {
	repo := &stubRepository{
		rows: []row{
			{
				Item: Item{Tconst: "tt1", TitleType: "movie"},
				Cursor: cursor{
					Sort:        SortPopular,
					Tconst:      "tt1",
					VotesKey:    intPtr(100),
					Fingerprint: "popular|movies|||||",
				},
			},
			{
				Item: Item{Tconst: "tt2", TitleType: "movie"},
				Cursor: cursor{
					Sort:        SortPopular,
					Tconst:      "tt2",
					VotesKey:    intPtr(90),
					Fingerprint: "popular|movies|||||",
				},
			},
		},
	}
	svc := NewService(repo)

	resp, err := svc.Discover(context.Background(), Request{
		TypeGroup: "movies",
		Sort:      SortPopular,
		Limit:     1,
	})
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if !repo.calledQuery {
		t.Fatal("expected repository call")
	}
	if repo.lastQuery.Fingerprint != "popular|movies|||||" {
		t.Fatalf("unexpected fingerprint %q", repo.lastQuery.Fingerprint)
	}
	if len(resp.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(resp.Items))
	}
	if !resp.Meta.HasMore {
		t.Fatal("expected hasMore")
	}
	if resp.Meta.NextCursor == nil || *resp.Meta.NextCursor == "" {
		t.Fatal("expected next cursor")
	}
	if resp.Meta.AppliedFilters.Type != "movies" {
		t.Fatalf("unexpected type %q", resp.Meta.AppliedFilters.Type)
	}
}

func intPtr(v int) *int {
	return &v
}
