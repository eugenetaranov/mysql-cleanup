package main

import (
	"database/sql"
	"time"
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
	ParseAndDisplayConfigFiltered(configPath string, config Config) error
}

// DataCleaner interface for data cleanup operations
type DataCleaner interface {
	CleanupData(config Config) (*CleanupStats, error)
	TruncateTables(db *sql.DB, tables []string) error
	UpdateTables(db *sql.DB, databaseName string, tableConfigs map[string]TableUpdateConfig) (*CleanupStats, error)
	UpdateTableData(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig) (int, error)
}

// FakeDataGenerator interface for generating fake data
type FakeDataGenerator interface {
	GenerateFakeValue(fakerType string) (interface{}, error)
}

// SchemaAwareFakeDataGenerator generates fake data based on database schema
type SchemaAwareFakeDataGenerator interface {
	GetColumnInfo(databaseName, tableName, columnName string, db *sql.DB) (*ColumnInfo, error)
	GenerateFakeValue(fakerType string, columnInfo *ColumnInfo) (interface{}, error)
}

// ColumnInfo represents database column metadata
type ColumnInfo struct {
	DataType     string
	MaxLength    *int
	IsNullable   bool
	DefaultValue *string
}

// CleanupStats represents the statistics from a cleanup operation
type CleanupStats struct {
	TotalRowsProcessed int
	TotalDuration      time.Duration
	TablesProcessed    int
}

// FileReader interface for file operations
type FileReader interface {
	ReadFile(filename string) ([]byte, error)
}

// Logger interface for logging operations
type Logger interface {
	Printf(format string, args ...interface{})
	Println(args ...interface{})
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	With(fields ...Field) Logger
}

// Field represents a key-value pair for structured logging
type Field struct {
	Key   string
	Value interface{}
}

// Field constructors for convenience
func String(key, value string) Field {
	return Field{Key: key, Value: value}
}

func Int(key string, value int) Field {
	return Field{Key: key, Value: value}
}

func Int64(key string, value int64) Field {
	return Field{Key: key, Value: value}
}

func Bool(key string, value bool) Field {
	return Field{Key: key, Value: value}
}

func Error(key string, err error) Field {
	return Field{Key: key, Value: err}
}

func Any(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}

// TableDataFetcher interface for fetching table data
type TableDataFetcher interface {
	FetchAndDisplayTableData(config Config) error
}

// S3Handler interface for S3 file operations
type S3Handler interface {
	DownloadS3File(s3URI string) (string, error)
	CleanupTempFile(filePath string) error
}

// Service struct that holds all dependencies
type Service struct {
	dbConnector   DatabaseConnector
	configParser  ConfigParser
	dataCleaner   DataCleaner
	fakeGenerator FakeDataGenerator
	fileReader    FileReader
	logger        Logger
	tableFetcher  TableDataFetcher
}
