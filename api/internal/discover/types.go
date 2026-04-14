package discover

type Item struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	StartYear     *int     `json:"startYear"`
	EndYear       *int     `json:"endYear"`
	Genres        []string `json:"genres"`
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Response struct {
	Items []Item `json:"items"`
	Meta  Meta   `json:"meta"`
}

type Meta struct {
	Sort           string  `json:"sort"`
	Limit          int     `json:"limit"`
	HasMore        bool    `json:"hasMore"`
	NextCursor     *string `json:"nextCursor,omitempty"`
	AppliedFilters Filter  `json:"appliedFilters"`
}

type Filter struct {
	Type      string   `json:"type"`
	Genres    []string `json:"genres"`
	YearFrom  *int     `json:"yearFrom,omitempty"`
	YearTo    *int     `json:"yearTo,omitempty"`
	MinVotes  *int     `json:"minVotes,omitempty"`
	MinRating *float64 `json:"minRating,omitempty"`
}

type Sort string

const (
	SortPopular  Sort = "popular"
	SortTopRated Sort = "top_rated"
	SortNewest   Sort = "newest"
	SortOldest   Sort = "oldest"
)

type Request struct {
	TypeGroup string
	Genres    []string
	YearFrom  *int
	YearTo    *int
	MinVotes  *int
	MinRating *float64
	Sort      Sort
	Limit     int
	Cursor    string
}

type cursor struct {
	Sort        Sort     `json:"sort"`
	Tconst      string   `json:"tconst"`
	VotesKey    *int     `json:"votesKey,omitempty"`
	YearKey     *int     `json:"yearKey,omitempty"`
	RatingKey   *float64 `json:"ratingKey,omitempty"`
	Fingerprint string   `json:"fingerprint"`
}

type query struct {
	TypeGroup   string
	Genres      []string
	YearFrom    *int
	YearTo      *int
	MinVotes    *int
	MinRating   *float64
	Sort        Sort
	Limit       int
	Fingerprint string
	Cursor      *cursor
}

type row struct {
	Item   Item
	Cursor cursor
}
