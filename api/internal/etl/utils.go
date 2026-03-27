package etl

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

func parseInt(raw string) *int {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	val, err := strconv.Atoi(raw)
	if err != nil {
		return nil
	}
	return &val
}

func parseFloat(raw string) *float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil
	}
	return &val
}

func splitList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "\\N" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseCharacters(raw string) []string {
	if raw == "" || raw == "\\N" {
		return nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err == nil {
		return out
	}
	return []string{raw}
}

func normalizeCharacters(raw string) []string {
	chars := parseCharacters(raw)
	if len(chars) == 0 {
		return nil
	}
	out := make([]string, 0, len(chars))
	for _, ch := range chars {
		if ch == "" {
			continue
		}
		parts := strings.Split(ch, ",")
		if len(parts) == 1 {
			ch = strings.TrimSpace(ch)
			if ch != "" {
				out = append(out, ch)
			}
			continue
		}
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			out = append(out, part)
		}
	}
	return dedupeCharacters(out)
}

func toJSONB(v any) (json.RawMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func intOrNil(val *int) any {
	if val == nil {
		return nil
	}
	return *val
}

func floatOrNil(val *float64) any {
	if val == nil {
		return nil
	}
	return *val
}

func dedupeStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func fieldByIndex(line string, index int) (string, bool) {
	if index < 0 {
		return "", false
	}
	start := 0
	for i := 0; i <= len(line); i++ {
		if i == len(line) || line[i] == '\t' {
			if index == 0 {
				return line[start:i], true
			}
			index--
			start = i + 1
		}
	}
	return "", false
}

func parseTconstID(raw string) (uint32, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 {
		return 0, false
	}
	if (raw[0] != 't' && raw[0] != 'T') || (raw[1] != 't' && raw[1] != 'T') {
		return 0, false
	}
	var val uint64
	for i := 2; i < len(raw); i++ {
		ch := raw[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		val = val*10 + uint64(ch-'0')
		if val > uint64(^uint32(0)) {
			return 0, false
		}
	}
	return uint32(val), true
}

func parseNconstID(raw string) (uint32, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 3 {
		return 0, false
	}
	if (raw[0] != 'n' && raw[0] != 'N') || (raw[1] != 'm' && raw[1] != 'M') {
		return 0, false
	}
	var val uint64
	for i := 2; i < len(raw); i++ {
		ch := raw[i]
		if ch < '0' || ch > '9' {
			return 0, false
		}
		val = val*10 + uint64(ch-'0')
		if val > uint64(^uint32(0)) {
			return 0, false
		}
	}
	return uint32(val), true
}

func akaTypeRank(aka Aka) int {
	for _, typ := range aka.Types {
		if strings.EqualFold(typ, "imdbDisplay") {
			return 0
		}
	}
	return 1
}

func akaOrderingOrMax(ordering int) int {
	if ordering > 0 {
		return ordering
	}
	return int(^uint(0) >> 1)
}

func sameTitle(a string, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	return strings.EqualFold(a, b)
}

func filterAkas(akas map[string][]Aka, primaryTitle string, originalTitle string) map[string][]Aka {
	if len(akas) == 0 {
		return akas
	}
	if strings.TrimSpace(primaryTitle) == "" && strings.TrimSpace(originalTitle) == "" {
		return akas
	}
	out := make(map[string][]Aka, len(akas))
	for region, list := range akas {
		filtered := make([]Aka, 0, len(list))
		for _, aka := range list {
			if sameTitle(aka.Title, primaryTitle) || sameTitle(aka.Title, originalTitle) {
				continue
			}
			filtered = append(filtered, aka)
		}
		if len(filtered) > 0 {
			out[region] = filtered
		}
	}
	return out
}

func seasonKeyFromPtr(val *int) seasonKey {
	if val == nil {
		return seasonKey{}
	}
	return seasonKey{HasValue: true, Value: *val}
}

func clampPositive(val int, def int) int {
	if val <= 0 {
		return def
	}
	return val
}

func clampInt(val int, min int, max int) int {
	if val < min {
		return min
	}
	if val > max {
		return max
	}
	return val
}

func computeTitlePopularity(numVotes *int, startYear *int, datasetYear int) int {
	const (
		voteWeight      = 0.85
		recencyWeight   = 0.15
		voteScale       = 6.0
		recencyDecayAge = 25.0
	)

	voteScore := 0.0
	if numVotes != nil && *numVotes > 0 {
		voteScore = math.Log10(float64(*numVotes)+1.0) / voteScale
		if voteScore > 1.0 {
			voteScore = 1.0
		}
	}

	recencyScore := 0.0
	if startYear != nil && *startYear > 0 {
		age := max(datasetYear-*startYear, 0)
		recencyScore = math.Exp(-float64(age) / recencyDecayAge)
	}

	score := (voteScore * voteWeight) + (recencyScore * recencyWeight)
	return clampInt(int(math.Round(score*100.0)), 0, 100)
}
