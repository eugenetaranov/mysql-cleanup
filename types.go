package main

import (
	"fmt"
	"strconv"
	"strings"
)

// YAMLConfig represents the structure of the YAML configuration file
type YAMLConfig struct {
	Databases map[string]DatabaseConfig `yaml:"databases"`
}

// DatabaseConfig represents the configuration for a specific database
type DatabaseConfig struct {
	Truncate []string                     `yaml:"truncate,omitempty"`
	Update   map[string]TableUpdateConfig `yaml:"update,omitempty"`
}

// TableUpdateConfig represents the configuration for updating a specific table
type TableUpdateConfig struct {
	Columns       map[string]string `yaml:"columns"`
	ExcludeClause string            `yaml:"exclude_clause,omitempty"`
}

// IDRange represents a range of IDs for manual range specification
type IDRange struct {
	Start    *int64 // nil means no start limit
	End      *int64 // nil means no end limit
	HasRange bool   // whether a range was specified
}

// parseHumanizedNumber parses numbers with K, M, B suffixes (e.g., "100K" = 100000)
func parseHumanizedNumber(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty number")
	}

	// Handle suffixes
	multiplier := int64(1)
	upperS := strings.ToUpper(s)

	if strings.HasSuffix(upperS, "K") {
		multiplier = 1000
		s = s[:len(s)-1]
	} else if strings.HasSuffix(upperS, "M") {
		multiplier = 1000000
		s = s[:len(s)-1]
	} else if strings.HasSuffix(upperS, "B") {
		multiplier = 1000000000
		s = s[:len(s)-1]
	}

	// Parse the base number
	baseNum, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number format: %s", s)
	}

	return baseNum * multiplier, nil
}

// ParseIDRange parses a range string like "0:1000", "1000:", ":100K", or "100K:1M" (colon is required)
func ParseIDRange(rangeStr string) (*IDRange, error) {
	if rangeStr == "" {
		return &IDRange{HasRange: false}, nil
	}

	// Check if it contains a colon (required for all range formats)
	if !strings.Contains(rangeStr, ":") {
		return nil, fmt.Errorf("invalid range format: %s (colon required, e.g., '0:1000', '1000:', ':100K', or '100K:1M')", rangeStr)
	}

	parts := strings.Split(rangeStr, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range format: %s (expected 'start:end' format)", rangeStr)
	}

	var start, end *int64

	// Parse start
	if parts[0] != "" {
		startVal, err := parseHumanizedNumber(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid start value: %s (%s)", parts[0], err)
		}
		start = &startVal
	}

	// Parse end
	if parts[1] != "" {
		endVal, err := parseHumanizedNumber(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid end value: %s (%s)", parts[1], err)
		}
		end = &endVal
	}

	return &IDRange{Start: start, End: end, HasRange: true}, nil
}

// BuildRangeWhereClause builds a WHERE clause for the ID range
func (r *IDRange) BuildRangeWhereClause(pkCol string) string {
	if !r.HasRange {
		return ""
	}

	var conditions []string

	if r.Start != nil {
		conditions = append(conditions, fmt.Sprintf("`%s` >= %d", pkCol, *r.Start))
	}

	if r.End != nil {
		conditions = append(conditions, fmt.Sprintf("`%s` <= %d", pkCol, *r.End))
	}

	if len(conditions) == 0 {
		return ""
	}

	return strings.Join(conditions, " AND ")
}

// String returns a human-readable representation of the range
func (r *IDRange) String() string {
	if !r.HasRange {
		return "no range"
	}

	if r.Start != nil && r.End != nil {
		return fmt.Sprintf("%d-%d", *r.Start, *r.End)
	} else if r.Start != nil {
		return fmt.Sprintf("%d+", *r.Start)
	} else if r.End != nil {
		return fmt.Sprintf("≤%d", *r.End)
	}
	return "invalid range"
}
