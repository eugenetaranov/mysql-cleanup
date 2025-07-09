package main

import (
	"database/sql"
)

// DatabaseConnector interface for database operations
type DatabaseConnector interface {
	Connect(config Config) (*sql.DB, error)
	Ping(db *sql.DB) error
	Close(db *sql.DB) error
}

// ConfigParser interface for configuration parsing
type ConfigParser interface {
	ParseConfig(configPath string) (*YAMLConfig, error)
	ParseAndDisplayConfig(configPath string) error
}

// DataCleaner interface for data cleanup operations
type DataCleaner interface {
	CleanupData(config Config) error
	TruncateTables(db *sql.DB, tables []string) error
	UpdateTables(db *sql.DB, tableConfigs map[string]TableUpdateConfig) error
	UpdateTableData(db *sql.DB, tableName string, tableConfig TableUpdateConfig) error
}

// FakeDataGenerator interface for generating fake data
type FakeDataGenerator interface {
	GenerateFakeValue(fakerType string) (interface{}, error)
}

// FileReader interface for file operations
type FileReader interface {
	ReadFile(filename string) ([]byte, error)
}

// Logger interface for logging operations
type Logger interface {
	Printf(format string, args ...interface{})
	Println(args ...interface{})
}

// TableDataFetcher interface for fetching table data
type TableDataFetcher interface {
	FetchAndDisplayTableData(config Config) error
}

// Service struct that holds all dependencies
type Service struct {
	dbConnector     DatabaseConnector
	configParser    ConfigParser
	dataCleaner     DataCleaner
	fakeGenerator   FakeDataGenerator
	fileReader      FileReader
	logger          Logger
	tableFetcher    TableDataFetcher
} 