package etl

type TitleBasics struct {
	Tconst         string   `json:"tconst"`
	TitleType      string   `json:"titleType"`
	PrimaryTitle   string   `json:"primaryTitle"`
	OriginalTitle  string   `json:"originalTitle"`
	IsAdult        bool     `json:"isAdult"`
	StartYear      *int     `json:"startYear"`
	EndYear        *int     `json:"endYear"`
	RuntimeMinutes *int     `json:"runtimeMinutes"`
	Genres         []string `json:"genres"`
}

type Rating struct {
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Aka struct {
	Title           string   `json:"title"`
	Language        string   `json:"language,omitempty"`
	Types           []string `json:"types,omitempty"`
	Attributes      []string `json:"attributes,omitempty"`
	IsOriginalTitle bool     `json:"isOriginalTitle"`
	Ordering        int      `json:"ordering,omitempty"`
}

type CastMember struct {
	Nconst     string   `json:"nconst"`
	Name       string   `json:"name"`
	Category   string   `json:"category"`
	Characters []string `json:"characters"`
}

type CrewMember struct {
	Nconst string `json:"nconst"`
	Name   string `json:"name"`
}

type Producer struct {
	Nconst   string `json:"nconst"`
	Name     string `json:"name"`
	Category string `json:"category"`
}

type TitleCrew struct {
	Directors []CrewMember `json:"directors"`
	Writers   []CrewMember `json:"writers"`
	Producers []Producer   `json:"producers"`
}

type Episode struct {
	Tconst        string   `json:"tconst"`
	EpisodeNumber *int     `json:"episodeNumber"`
	PrimaryTitle  string   `json:"primaryTitle"`
	StartYear     *int     `json:"startYear"`
	AverageRating *float64 `json:"averageRating"`
	NumVotes      *int     `json:"numVotes"`
}

type Season struct {
	SeasonNumber *int      `json:"seasonNumber"`
	Episodes     []Episode `json:"episodes"`
}

type TitleData struct {
	Basics   TitleBasics      `json:"basics"`
	Akas     map[string][]Aka `json:"akas"`
	Ratings  Rating           `json:"ratings"`
	Cast     []CastMember     `json:"cast"`
	Crew     TitleCrew        `json:"crew"`
	Episodes []Season         `json:"episodes"`
}

type principalRow struct {
	Tconst     string
	Ordering   int
	Nconst     string
	Category   string
	Characters string
}

type crewLists struct {
	Directors []string
	Writers   []string
}

type seasonKey struct {
	HasValue bool
	Value    int
}
