package main

// YAMLConfig represents the structure of the YAML configuration file
type YAMLConfig struct {
	Databases map[string]DatabaseConfig `yaml:"databases"`
}

// DatabaseConfig represents the configuration for a specific database
type DatabaseConfig struct {
	Truncate []string                    `yaml:"truncate,omitempty"`
	Update   map[string]TableUpdateConfig `yaml:"update,omitempty"`
}

// TableUpdateConfig represents the configuration for updating a specific table
type TableUpdateConfig struct {
	Columns       map[string]string `yaml:"columns"`
	ExcludeClause string            `yaml:"exclude_clause,omitempty"`
} 