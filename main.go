package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Host     string
	User     string
	Port     string
	Password string
	Config   string
	DB       string
	Table    string
}

// YAML Configuration Structures
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

func main() {
	// Load .env file if it exists
	if err := godotenv.Load(); err != nil {
		// It's okay if .env doesn't exist
		log.Println("No .env file found, continuing...")
	}

	// Define flags
	var config Config

	flag.StringVar(&config.Host, "host", getEnvWithDefault("HOST", "localhost"), "Database host")
	flag.StringVar(&config.User, "user", getEnvWithDefault("USER", "root"), "Database user")
	flag.StringVar(&config.Port, "port", getEnvWithDefault("PORT", "3306"), "Database port")
	flag.StringVar(&config.Password, "password", getEnvWithDefault("PASSWORD", ""), "Database password")
	flag.StringVar(&config.Config, "config", getEnvWithDefault("CONFIG", ""), "Configuration file path")
	flag.StringVar(&config.DB, "db", getEnvWithDefault("DB", ""), "Database name")
	flag.StringVar(&config.Table, "table", getEnvWithDefault("TABLE", ""), "Table name")

	// Parse flags
	flag.Parse()

	// Output the provided arguments
	fmt.Println("MySQL Cleanup CLI")
	fmt.Println("==================")
	fmt.Printf("Host: %s\n", config.Host)
	fmt.Printf("User: %s\n", config.User)
	fmt.Printf("Port: %s\n", config.Port)
	fmt.Printf("Password: %s\n", maskPassword(config.Password))
	fmt.Printf("Config: %s\n", config.Config)
	fmt.Printf("Database: %s\n", config.DB)
	fmt.Printf("Table: %s\n", config.Table)

	// Parse and display YAML configuration if provided
	if config.Config != "" {
		fmt.Println("\nYAML Configuration:")
		fmt.Println("===================")
		if err := parseAndDisplayConfig(config.Config); err != nil {
			log.Printf("Error parsing config file: %v\n", err)
		}
	}

	// Fetch and display table data if database and table are specified
	if config.DB != "" && config.Table != "" {
		fmt.Println("\nTable Data:")
		fmt.Println("===========")
		if err := fetchAndDisplayTableData(config); err != nil {
			log.Printf("Error fetching table data: %v\n", err)
		}
	}
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

// getEnvWithDefault returns the environment variable value or a default if not set
func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// maskPassword returns asterisks for password display
func maskPassword(password string) string {
	if password == "" {
		return "<empty>"
	}
	return "********"
}
