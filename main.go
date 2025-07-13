package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Host      string
	User      string
	Port      string
	Password  string
	Config    string
	DB        string
	Table     string
	AllTables bool
	Debug     bool
	Workers   int
	BatchSize string // Batch size for updates (e.g., "1", "1K", "10K", "100K" - supports K/M/B suffixes)
	Range     string // ID range specification (e.g., "0:1000", "1000:", ":100K", "100K:1M" - colon required)
}

// createService creates and wires up all dependencies
func createService(debug bool, workers, batchSize int) *Service {
	// Create concrete implementations
	dbConnector := &MySQLConnector{}
	fileReader := &OSFileReader{}
	logger := &StdLogger{} // Use simple logger without stack traces
	s3Handler := NewS3Handler(logger)
	configParser := NewYAMLConfigParser(fileReader, s3Handler, logger)
	fakeGenerator := NewGofakeitGenerator() // Use constructor instead of &GofakeitGenerator{}
	schemaAwareGenerator := NewSchemaAwareGofakeitGenerator(logger)
	dataCleaner := NewDataCleanupService(dbConnector, configParser, fakeGenerator, schemaAwareGenerator, logger, workers, batchSize)
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
		// Note: We can't use service.logger here as service isn't created yet
		log.Println("No .env file found, continuing...")
	}

	log.Println("Starting MySQL Cleanup CLI...")

	// Define flags
	var config Config

	flag.StringVar(&config.Host, "host", getEnvWithDefault("HOST", "localhost"), "Database host")
	flag.StringVar(&config.User, "user", getEnvWithDefault("USER", ""), "Database user")
	flag.StringVar(&config.Port, "port", getEnvWithDefault("PORT", "3306"), "Database port")
	flag.StringVar(&config.Password, "password", getEnvWithDefault("PASSWORD", ""), "Database password")
	flag.StringVar(&config.Config, "config", getEnvWithDefault("CONFIG", ""), "Configuration file path")
	flag.StringVar(&config.DB, "db", getEnvWithDefault("DB", ""), "Database name")
	flag.StringVar(&config.Table, "table", getEnvWithDefault("TABLE", ""), "Table name")
	flag.BoolVar(&config.AllTables, "all-tables", false, "Process all tables in the database")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug logging")
	flag.IntVar(&config.Workers, "workers", 1, "Number of worker goroutines (default: 1)")
	flag.StringVar(&config.BatchSize, "batch-size", "1", "Batch size for updates (e.g., '1', '1K', '10K', '100K' - supports K/M/B suffixes)")
	flag.StringVar(&config.Range, "range", "", "ID range to process (e.g., '0:1000' for IDs 0-1000, '1000:' for IDs 1000+, ':100K' for IDs up to 100K, '100K:1M' for IDs 100K-1M) - colon required")

	// Parse flags
	flag.Parse()

	// Parse humanized batch size
	batchSize, err := parseHumanizedBatchSize(config.BatchSize)
	if err != nil {
		log.Fatalf("Invalid batch size: %v", err)
	}

	// Create service with all dependencies
	service := createService(config.Debug, config.Workers, batchSize)
	service.logger.Debug("Service created successfully")

	// Output the provided arguments
	service.logger.Debug("MySQL Cleanup CLI")
	service.logger.Debug("==================")
	service.logger.Debug(fmt.Sprintf("Configuration - host: %s, user: %s, port: %s, password: %s, config: %s, database: %s, range: %s",
		config.Host, config.User, config.Port, maskPassword(config.Password), config.Config, config.DB, config.Range))
	if config.AllTables {
		service.logger.Debug("Mode: All tables")
	} else {
		service.logger.Debug(fmt.Sprintf("Mode: Single table - table: %s", config.Table))
	}

	// Parse and display YAML configuration if provided
	if config.Config != "" {
		service.logger.Debug(fmt.Sprintf("Parsing YAML configuration - config_path: %s", config.Config))
		service.logger.Debug("YAML Configuration:")
		service.logger.Debug("===================")
		if err := service.configParser.ParseAndDisplayConfigFiltered(config.Config, config); err != nil {
			service.logger.Error(fmt.Sprintf("Error parsing config file - error: %s", err))
		}

		// Validate arguments
		service.logger.Debug("Validating arguments")
		if config.DB == "" {
			service.logger.Error("Error: -db argument is required")
			os.Exit(1)
		}

		if config.User == "" {
			service.logger.Error("Error: -user argument is required")
			os.Exit(1)
		}

		if !config.AllTables && config.Table == "" {
			service.logger.Error("Error: Either -table or -all-tables argument is required")
			os.Exit(1)
		}
		service.logger.Debug("Arguments validated successfully")

		// Perform the actual data cleanup
		service.logger.Debug("Starting data cleanup process")
		service.logger.Debug("Performing Data Cleanup:")
		service.logger.Debug("========================")
		stats, err := service.dataCleaner.CleanupData(config)
		if err != nil {
			// Provide specific error messages based on failure type
			if strings.Contains(err.Error(), "connection timeout") || strings.Contains(err.Error(), "i/o timeout") {
				service.logger.Error("Database connection timeout - server is not responding")
			} else if strings.Contains(err.Error(), "Access denied") || strings.Contains(err.Error(), "authentication") {
				service.logger.Error("Database authentication failed - check username/password")
			} else if strings.Contains(err.Error(), "connection refused") {
				service.logger.Error("Database connection refused - check if server is running")
			} else if strings.Contains(err.Error(), "no such host") || strings.Contains(err.Error(), "unknown host") {
				service.logger.Error("Database host not found - check hostname/IP address")
			} else if strings.Contains(err.Error(), "Unknown database") {
				service.logger.Error("Database not found - check database name")
			} else if strings.Contains(err.Error(), "no primary key") {
				service.logger.Error("Table structure issue - the specified table does not have a primary key defined")
			} else {
				service.logger.Error(fmt.Sprintf("Data cleanup failed: %s", err))
			}
		} else {
			service.logger.Info(fmt.Sprintf("Data cleanup completed successfully! total_rows_processed: %d, tables_processed: %d, total_duration: %s",
				stats.TotalRowsProcessed, stats.TablesProcessed, FormatDuration(stats.TotalDuration)))
		}
	}

	// Table data is already shown via sample data during batch processing
	// No need to dump the entire table afterwards
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

// parseHumanizedBatchSize parses batch sizes with K, M, B suffixes (e.g., "1K" = 1000)
func parseHumanizedBatchSize(batchSizeStr string) (int, error) {
	batchSizeStr = strings.TrimSpace(batchSizeStr)
	if batchSizeStr == "" {
		return 0, fmt.Errorf("empty batch size")
	}

	// Handle suffixes
	multiplier := 1
	upperStr := strings.ToUpper(batchSizeStr)

	if strings.HasSuffix(upperStr, "K") {
		multiplier = 1000
		batchSizeStr = batchSizeStr[:len(batchSizeStr)-1]
	} else if strings.HasSuffix(upperStr, "M") {
		multiplier = 1000000
		batchSizeStr = batchSizeStr[:len(batchSizeStr)-1]
	} else if strings.HasSuffix(upperStr, "B") {
		multiplier = 1000000000
		batchSizeStr = batchSizeStr[:len(batchSizeStr)-1]
	}

	// Parse the base number
	baseNum, err := strconv.Atoi(batchSizeStr)
	if err != nil {
		return 0, fmt.Errorf("invalid batch size format: %s", batchSizeStr)
	}

	result := baseNum * multiplier
	if result <= 0 {
		return 0, fmt.Errorf("batch size must be positive")
	}

	return result, nil
}
