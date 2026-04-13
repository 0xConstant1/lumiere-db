package search

type SearchItem struct {
	Tconst        string   `json:"tconst"`
	TitleType     string   `json:"titleType"`
	StartYear     *int     `json:"startYear"`
	PrimaryTitle  string   `json:"primaryTitle"`
	OriginalTitle string   `json:"originalTitle"`
	AkaTitles     []string `json:"akaTitles"`
	Popularity    int      `json:"popularity"`
	Similarity    float64  `json:"similarity"`
}

type SearchResponse struct {
	Items []SearchItem `json:"items"`
}

type SuggestItem struct {
	Tconst        string  `json:"tconst"`
	TitleType     string  `json:"titleType"`
	StartYear     *int    `json:"startYear"`
	PrimaryTitle  string  `json:"primaryTitle"`
	OriginalTitle string  `json:"originalTitle"`
	Popularity    int     `json:"popularity"`
	Similarity    float64 `json:"similarity"`
}

type SuggestResponse struct {
	Items []SuggestItem `json:"items"`
}

type SearchRequest struct {
	Query     string
	TypeGroup string
	Limit     int
}

type SuggestRequest struct {
	Query     string
	TypeGroup string
	Limit     int
}

type searchQuery struct {
	Query        string
	TypeList     []string
	Limit        int
	EnablePhrase bool
}

type suggestQuery struct {
	Query              string
	TypeList           []string
	RegexPattern       string
	Limit              int
	EnablePhrasePrefix bool
}
