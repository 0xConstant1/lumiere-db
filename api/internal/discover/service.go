package discover

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

type Repository interface {
	List(ctx context.Context, query query) ([]row, error)
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

func (s *Service) Discover(ctx context.Context, req Request) (Response, error) {
	fingerprint := filterFingerprint(req.Sort, req.TypeGroup, req.Genres, req.YearFrom, req.YearTo, req.MinVotes, req.MinRating)

	var cur *cursor
	if raw := strings.TrimSpace(req.Cursor); raw != "" {
		parsed, err := decodeCursor(raw)
		if err != nil {
			return Response{}, &ValidationError{Message: "invalid cursor"}
		}
		if parsed.Sort != req.Sort {
			return Response{}, &ValidationError{Message: "cursor sort does not match requested sort"}
		}
		if parsed.Fingerprint != fingerprint {
			return Response{}, &ValidationError{Message: "cursor does not match requested filters"}
		}
		cur = &parsed
	}

	rows, err := s.repo.List(ctx, query{
		TypeGroup:   req.TypeGroup,
		Genres:      req.Genres,
		YearFrom:    req.YearFrom,
		YearTo:      req.YearTo,
		MinVotes:    req.MinVotes,
		MinRating:   req.MinRating,
		Sort:        req.Sort,
		Limit:       req.Limit,
		Fingerprint: fingerprint,
		Cursor:      cur,
	})
	if err != nil {
		return Response{}, err
	}

	hasMore := len(rows) > req.Limit
	if hasMore {
		rows = rows[:req.Limit]
	}

	items := make([]Item, 0, len(rows))
	for _, row := range rows {
		items = append(items, row.Item)
	}

	var nextCursor *string
	if hasMore && len(rows) > 0 {
		encoded, err := encodeCursor(rows[len(rows)-1].Cursor)
		if err != nil {
			return Response{}, err
		}
		nextCursor = &encoded
	}

	return Response{
		Items: items,
		Meta: Meta{
			Sort:       string(req.Sort),
			Limit:      req.Limit,
			HasMore:    hasMore,
			NextCursor: nextCursor,
			AppliedFilters: Filter{
				Type:      req.TypeGroup,
				Genres:    req.Genres,
				YearFrom:  req.YearFrom,
				YearTo:    req.YearTo,
				MinVotes:  req.MinVotes,
				MinRating: req.MinRating,
			},
		},
	}, nil
}

func IsValidationError(err error) bool {
	var validationErr *ValidationError
	return errors.As(err, &validationErr)
}

func encodeCursor(cur cursor) (string, error) {
	data, err := json.Marshal(cur)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor(raw string) (cursor, error) {
	var cur cursor
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cur, err
	}
	if err := json.Unmarshal(data, &cur); err != nil {
		return cur, err
	}
	if cur.Tconst == "" || cur.Sort == "" || cur.Fingerprint == "" {
		return cur, errors.New("invalid cursor payload")
	}
	return cur, nil
}

func filterFingerprint(
	sortMode Sort,
	typeGroup string,
	genres []string,
	yearFrom *int,
	yearTo *int,
	minVotes *int,
	minRating *float64,
) string {
	yearFromToken := ""
	if yearFrom != nil {
		yearFromToken = strconv.Itoa(*yearFrom)
	}
	yearToToken := ""
	if yearTo != nil {
		yearToToken = strconv.Itoa(*yearTo)
	}
	minVotesToken := ""
	if minVotes != nil {
		minVotesToken = strconv.Itoa(*minVotes)
	}
	minRatingToken := ""
	if minRating != nil {
		minRatingToken = strconv.FormatFloat(*minRating, 'f', 4, 64)
	}
	return strings.Join([]string{
		string(sortMode),
		typeGroup,
		strings.Join(genres, ","),
		yearFromToken,
		yearToToken,
		minVotesToken,
		minRatingToken,
	}, "|")
}
