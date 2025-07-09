package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
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

// createService creates and wires up all dependencies
func createService() *Service {
	// Create concrete implementations
	dbConnector := &MySQLConnector{}
	fileReader := &OSFileReader{}
	logger := NewZapLogger() // Use zap logger instead of std logger
	configParser := NewYAMLConfigParser(fileReader, logger)
	fakeGenerator := &GofakeitGenerator{}
	dataCleaner := NewDataCleanupService(dbConnector, configParser, fakeGenerator, logger)
	tableFetcher := NewMySQLTableFetcher(dbConnector, logger)

	// Create and return the service
	return &Service{
		dbConnector:   dbConnector,
		configParser:  configParser,
		dataCleaner:   dataCleaner,
		fakeGenerator: fakeGenerator,
		fileReader:    fileReader,
		logger:        logger,
		tableFetcher:  tableFetcher,
	}
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

	// Create service with all dependencies
	service := createService()

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
		if err := service.configParser.ParseAndDisplayConfig(config.Config); err != nil {
			log.Printf("Error parsing config file: %v\n", err)
		}

		// Perform the actual data cleanup
		fmt.Println("\nPerforming Data Cleanup:")
		fmt.Println("========================")
		if err := service.dataCleaner.CleanupData(config); err != nil {
			log.Printf("Error during data cleanup: %v\n", err)
		} else {
			fmt.Println("Data cleanup completed successfully!")
		}
	}

	// Fetch and display table data if database and table are specified
	if config.DB != "" && config.Table != "" {
		fmt.Println("\nTable Data:")
		fmt.Println("===========")
		if err := service.tableFetcher.FetchAndDisplayTableData(config); err != nil {
			log.Printf("Error fetching table data: %v\n", err)
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
