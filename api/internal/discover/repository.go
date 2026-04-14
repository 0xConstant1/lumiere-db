package discover

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PGRepository struct {
	pool *pgxpool.Pool
}

func NewPGRepository(pool *pgxpool.Pool) *PGRepository {
	return &PGRepository{pool: pool}
}

func (r *PGRepository) List(ctx context.Context, q query) ([]row, error) {
	args := make([]any, 0, 16)
	args = append(args, q.TypeGroup)
	param := 2
	var where strings.Builder
	where.WriteString("WHERE d.type_group = $1")

	switch q.Sort {
	case SortTopRated:
		where.WriteString(" AND d.average_rating IS NOT NULL AND d.num_votes IS NOT NULL")
	case SortNewest, SortOldest:
		where.WriteString(" AND d.start_year IS NOT NULL AND d.num_votes IS NOT NULL")
	default:
		where.WriteString(" AND d.num_votes IS NOT NULL")
	}

	if q.YearFrom != nil {
		where.WriteString(fmt.Sprintf(" AND d.start_year >= $%d", param))
		args = append(args, *q.YearFrom)
		param++
	}
	if q.YearTo != nil {
		where.WriteString(fmt.Sprintf(" AND d.start_year <= $%d", param))
		args = append(args, *q.YearTo)
		param++
	}
	if q.MinVotes != nil {
		where.WriteString(fmt.Sprintf(" AND d.num_votes >= $%d", param))
		args = append(args, *q.MinVotes)
		param++
	}
	if q.MinRating != nil {
		where.WriteString(fmt.Sprintf(" AND d.average_rating >= $%d::numeric", param))
		args = append(args, *q.MinRating)
		param++
	}
	if len(q.Genres) > 0 {
		where.WriteString(fmt.Sprintf(" AND d.genres @> $%d::text[]", param))
		args = append(args, q.Genres)
		param++
	}

	if q.Cursor != nil {
		switch q.Sort {
		case SortTopRated:
			if q.Cursor.RatingKey == nil || q.Cursor.VotesKey == nil {
				return nil, &ValidationError{Message: "invalid cursor"}
			}
			where.WriteString(fmt.Sprintf(" AND (d.average_rating, d.num_votes, d.tconst) < ($%d::numeric, $%d, $%d)", param, param+1, param+2))
			args = append(args, *q.Cursor.RatingKey, *q.Cursor.VotesKey, q.Cursor.Tconst)
			param += 3
		case SortNewest:
			if q.Cursor.YearKey == nil || q.Cursor.VotesKey == nil {
				return nil, &ValidationError{Message: "invalid cursor"}
			}
			where.WriteString(fmt.Sprintf(" AND (d.start_year, d.num_votes, d.tconst) < ($%d, $%d, $%d)", param, param+1, param+2))
			args = append(args, *q.Cursor.YearKey, *q.Cursor.VotesKey, q.Cursor.Tconst)
			param += 3
		case SortOldest:
			if q.Cursor.YearKey == nil || q.Cursor.VotesKey == nil {
				return nil, &ValidationError{Message: "invalid cursor"}
			}
			where.WriteString(fmt.Sprintf(" AND (d.start_year > $%d OR (d.start_year = $%d AND (d.num_votes, d.tconst) < ($%d, $%d)))", param, param, param+1, param+2))
			args = append(args, *q.Cursor.YearKey, *q.Cursor.VotesKey, q.Cursor.Tconst)
			param += 3
		default:
			if q.Cursor.VotesKey == nil {
				return nil, &ValidationError{Message: "invalid cursor"}
			}
			where.WriteString(fmt.Sprintf(" AND (d.num_votes, d.tconst) < ($%d, $%d)", param, param+1))
			args = append(args, *q.Cursor.VotesKey, q.Cursor.Tconst)
			param += 2
		}
	}

	sqlLimit := q.Limit + 1
	args = append(args, sqlLimit)
	sql := fmt.Sprintf(`
SELECT d.tconst, d.title_type, d.primary_title, d.original_title, d.start_year, d.end_year,
       d.genres, d.average_rating::float8, d.num_votes
FROM discover_core d
%s
ORDER BY %s
LIMIT $%d`, where.String(), orderClause(q.Sort), param)

	rows, err := r.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	results := make([]row, 0, sqlLimit)
	for rows.Next() {
		var (
			tconst        string
			titleType     string
			primaryTitle  string
			originalTitle string
			startYear     pgtype.Int4
			endYear       pgtype.Int4
			genresArr     []string
			avgRating     pgtype.Float8
			numVotes      pgtype.Int4
		)
		if err := rows.Scan(
			&tconst,
			&titleType,
			&primaryTitle,
			&originalTitle,
			&startYear,
			&endYear,
			&genresArr,
			&avgRating,
			&numVotes,
		); err != nil {
			return nil, err
		}

		if genresArr == nil {
			genresArr = []string{}
		}

		cur := cursor{
			Sort:        q.Sort,
			Tconst:      tconst,
			Fingerprint: q.Fingerprint,
			VotesKey:    intPtrFromPg(numVotes),
		}
		if cur.VotesKey == nil {
			return nil, errors.New("discover cursor invariant: missing votes key")
		}

		switch q.Sort {
		case SortTopRated:
			cur.RatingKey = floatPtrFromPg(avgRating)
			if cur.RatingKey == nil {
				return nil, errors.New("discover cursor invariant: missing rating key")
			}
		case SortNewest, SortOldest:
			cur.YearKey = intPtrFromPg(startYear)
			if cur.YearKey == nil {
				return nil, errors.New("discover cursor invariant: missing year key")
			}
		}

		results = append(results, row{
			Item: Item{
				Tconst:        tconst,
				TitleType:     titleType,
				PrimaryTitle:  primaryTitle,
				OriginalTitle: originalTitle,
				StartYear:     intPtrFromPg(startYear),
				EndYear:       intPtrFromPg(endYear),
				Genres:        genresArr,
				AverageRating: floatPtrFromPg(avgRating),
				NumVotes:      intPtrFromPg(numVotes),
			},
			Cursor: cur,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func orderClause(sortMode Sort) string {
	switch sortMode {
	case SortTopRated:
		return "d.average_rating DESC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	case SortNewest:
		return "d.start_year DESC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	case SortOldest:
		return "d.start_year ASC NULLS LAST, d.num_votes DESC NULLS LAST, d.tconst DESC"
	default:
		return "d.num_votes DESC NULLS LAST, d.tconst DESC"
	}
}

func intPtrFromPg(val pgtype.Int4) *int {
	if !val.Valid {
		return nil
	}
	v := int(val.Int32)
	return &v
}

func floatPtrFromPg(val pgtype.Float8) *float64 {
	if !val.Valid {
		return nil
	}
	v := val.Float64
	return &v
}
