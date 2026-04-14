package etl

import (
	"fmt"
	"strings"
	"time"
)

type searchCopyRow struct {
	Tconst        string
	TitleType     string
	StartYear     *int
	PrimaryTitle  string
	OriginalTitle string
	AkaTitles     []string
	Popularity    int
}

type titlesNextCopySource struct {
	cfg              Config
	datasetDate      time.Time
	tconsts          []string
	basics           map[string]TitleBasics
	ratings          map[string]Rating
	akasByTitle      map[string]map[string][]Aka
	actorsByTitle    map[string][]principalRow
	producersByTitle map[string][]principalRow
	crewByTitle      map[string]crewLists
	episodesByTitle  map[string][]Season
	names            map[string]string
	searchRows       *[]searchCopyRow

	idx      int
	inserted int
	row      []any
	err      error
}

func newTitlesNextCopySource(
	cfg Config,
	datasetDate time.Time,
	tconsts []string,
	basics map[string]TitleBasics,
	ratings map[string]Rating,
	akasByTitle map[string]map[string][]Aka,
	actorsByTitle map[string][]principalRow,
	producersByTitle map[string][]principalRow,
	crewByTitle map[string]crewLists,
	episodesByTitle map[string][]Season,
	names map[string]string,
	searchRows *[]searchCopyRow,
) *titlesNextCopySource {
	return &titlesNextCopySource{
		cfg:              cfg,
		datasetDate:      datasetDate,
		tconsts:          tconsts,
		basics:           basics,
		ratings:          ratings,
		akasByTitle:      akasByTitle,
		actorsByTitle:    actorsByTitle,
		producersByTitle: producersByTitle,
		crewByTitle:      crewByTitle,
		episodesByTitle:  episodesByTitle,
		names:            names,
		searchRows:       searchRows,
		row:              make([]any, 14),
	}
}

func (s *titlesNextCopySource) Next() bool {
	if s.err != nil {
		return false
	}
	for s.idx < len(s.tconsts) {
		tconst := s.tconsts[s.idx]
		s.idx++

		basic, ok := s.basics[tconst]
		if !ok {
			continue
		}

		rating := s.ratings[tconst]
		akas := s.akasByTitle[tconst]
		if akas == nil {
			akas = map[string][]Aka{}
		} else {
			akas = filterAkas(akas, basic.PrimaryTitle, basic.OriginalTitle)
			if akas == nil {
				akas = map[string][]Aka{}
			}
		}

		cast := buildCast(s.actorsByTitle[tconst], s.names)
		if cast == nil {
			cast = []CastMember{}
		}

		producers := buildProducers(s.producersByTitle[tconst], s.names)
		if producers == nil {
			producers = []Producer{}
		}

		crewLists := s.crewByTitle[tconst]
		directors := buildCrewMembers(crewLists.Directors, s.names)
		writers := buildCrewMembers(crewLists.Writers, s.names)
		if directors == nil {
			directors = []CrewMember{}
		}
		if writers == nil {
			writers = []CrewMember{}
		}

		episodes := s.episodesByTitle[tconst]
		if episodes == nil {
			episodes = []Season{}
		}

		data := TitleData{
			Basics:  basic,
			Akas:    akas,
			Ratings: rating,
			Cast:    cast,
			Crew: TitleCrew{
				Directors: directors,
				Writers:   writers,
				Producers: producers,
			},
			Episodes: episodes,
		}

		jsonb, err := toJSONB(data)
		if err != nil {
			s.err = fmt.Errorf("jsonb %s: %w", tconst, err)
			return false
		}

		s.row[0] = tconst
		s.row[1] = basic.TitleType
		s.row[2] = basic.PrimaryTitle
		s.row[3] = basic.OriginalTitle
		s.row[4] = intOrNil(basic.StartYear)
		s.row[5] = intOrNil(basic.EndYear)
		s.row[6] = basic.IsAdult
		s.row[7] = intOrNil(basic.RuntimeMinutes)
		s.row[8] = basic.Genres
		s.row[9] = floatOrNil(rating.AverageRating)
		s.row[10] = intOrNil(rating.NumVotes)
		s.row[11] = jsonb
		s.row[12] = s.datasetDate
		s.row[13] = s.cfg.SchemaVersion

		if basic.TitleType != "tvepisode" {
			akaTitles := make([]string, 0)
			for _, regionEntries := range akas {
				for _, aka := range regionEntries {
					akaTitles = append(akaTitles, aka.Title)
				}
			}
			akaTitles = dedupeStrings(akaTitles)
			*s.searchRows = append(*s.searchRows, searchCopyRow{
				Tconst:        tconst,
				TitleType:     basic.TitleType,
				StartYear:     basic.StartYear,
				PrimaryTitle:  basic.PrimaryTitle,
				OriginalTitle: basic.OriginalTitle,
				AkaTitles:     akaTitles,
				Popularity:    computeTitlePopularity(rating.NumVotes, basic.StartYear, s.datasetDate.Year()),
			})
		}

		s.inserted++
		return true
	}
	return false
}

func (s *titlesNextCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *titlesNextCopySource) Err() error {
	return s.err
}

func (s *titlesNextCopySource) Inserted() int {
	return s.inserted
}

type searchRowsCopySource struct {
	rows     []searchCopyRow
	idx      int
	inserted int
	row      []any
}

func newSearchRowsCopySource(rows []searchCopyRow) *searchRowsCopySource {
	return &searchRowsCopySource{
		rows: rows,
		row:  make([]any, 7),
	}
}

func (s *searchRowsCopySource) Next() bool {
	if s.idx >= len(s.rows) {
		return false
	}
	item := s.rows[s.idx]
	s.idx++

	s.row[0] = item.Tconst
	s.row[1] = item.TitleType
	s.row[2] = intOrNil(item.StartYear)
	s.row[3] = item.PrimaryTitle
	s.row[4] = item.OriginalTitle
	s.row[5] = item.AkaTitles
	s.row[6] = item.Popularity

	s.inserted++
	return true
}

func (s *searchRowsCopySource) Values() ([]any, error) {
	return s.row, nil
}

func (s *searchRowsCopySource) Err() error {
	return nil
}

func (s *searchRowsCopySource) Inserted() int {
	return s.inserted
}

func buildCast(rows []principalRow, names map[string]string) []CastMember {
	if len(rows) == 0 {
		return nil
	}
	cast := make([]CastMember, 0, len(rows))
	index := make(map[string]int, len(rows))
	for _, row := range rows {
		key := row.Nconst + "|" + row.Category
		chars := normalizeCharacters(row.Characters)
		if idx, ok := index[key]; ok {
			if len(chars) > 0 {
				cast[idx].Characters = mergeDedupStrings(cast[idx].Characters, chars)
			}
			continue
		}
		cast = append(cast, CastMember{
			Nconst:     row.Nconst,
			Name:       names[row.Nconst],
			Category:   row.Category,
			Characters: chars,
		})
		index[key] = len(cast) - 1
	}
	return cast
}

func mergeDedupStrings(base []string, add []string) []string {
	if len(add) == 0 {
		return base
	}
	if len(base) == 0 {
		return add
	}
	combined := make([]string, 0, len(base)+len(add))
	combined = append(combined, base...)
	combined = append(combined, add...)
	return dedupeCharacters(combined)
}

func dedupeCharacters(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		key := normalizeCharacterKey(value)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeCharacterKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(value))
	space := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			space = false
			continue
		}
		if !space {
			b.WriteByte(' ')
			space = true
		}
	}
	return strings.TrimSpace(b.String())
}

func buildProducers(rows []principalRow, names map[string]string) []Producer {
	if len(rows) == 0 {
		return nil
	}
	out := make([]Producer, 0, len(rows))
	for _, row := range rows {
		out = append(out, Producer{
			Nconst:   row.Nconst,
			Name:     names[row.Nconst],
			Category: row.Category,
		})
	}
	return out
}

func buildCrewMembers(nconsts []string, names map[string]string) []CrewMember {
	if len(nconsts) == 0 {
		return nil
	}
	out := make([]CrewMember, 0, len(nconsts))
	for _, nconst := range nconsts {
		out = append(out, CrewMember{
			Nconst: nconst,
			Name:   names[nconst],
		})
	}
	return out
}
