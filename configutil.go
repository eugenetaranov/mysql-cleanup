package main

import (
	"fmt"
	"io/ioutil"
	"gopkg.in/yaml.v3"
)

type YAMLConfig struct {
	Databases map[string]DatabaseConfig `yaml:"databases"`
}

type DatabaseConfig struct {
	Truncate []string                    `yaml:"truncate,omitempty"`
	Update   map[string]TableUpdateConfig `yaml:"update,omitempty"`
}

type TableUpdateConfig struct {
	Columns       map[string]string `yaml:"columns"`
	ExcludeClause string            `yaml:"exclude_clause,omitempty"`
}

// parseAndDisplayConfig reads and parses the YAML configuration file
func parseAndDisplayConfig(configPath string) error {
	// Read the config file
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML
	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		return fmt.Errorf("failed to parse YAML: %w", err)
	}

	// Display the parsed configuration
	displayConfig(yamlConfig)
	return nil
}

// displayConfig prints the parsed configuration in a readable format
func displayConfig(config YAMLConfig) {
	for dbName, dbConfig := range config.Databases {
		fmt.Printf("\nDatabase: %s\n", dbName)
		fmt.Printf("  Truncate tables: %v\n", dbConfig.Truncate)
		
		if len(dbConfig.Update) > 0 {
			fmt.Printf("  Update tables:\n")
			for tableName, tableConfig := range dbConfig.Update {
				fmt.Printf("    %s:\n", tableName)
				fmt.Printf("      Columns to update:\n")
				for column, value := range tableConfig.Columns {
					fmt.Printf("        %s: %s\n", column, value)
				}
				if tableConfig.ExcludeClause != "" {
					fmt.Printf("      Exclude clause: %s\n", tableConfig.ExcludeClause)
				}
			}
		}
	}
} 