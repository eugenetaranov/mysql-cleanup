package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
	"mysql_cleanup/internal/cleanup"
)

// Version will be set by the linker during build
var version = "dev"

// Config holds the command line configuration
type Config = cleanup.Config

// createService creates and wires up all dependencies
func createService(debug bool, workers int, batchSizeStr string, logFile string) *cleanup.Service {
	batchSize, _ := parseHumanizedBatchSize(batchSizeStr)
	// Create concrete implementations
	dbConnector := &cleanup.MySQLConnector{}
	fileReader := &cleanup.OSFileReader{}

	// Create logger based on debug and log file options
	var logger cleanup.Logger
	if logFile != "" {
		// Create multi-logger that writes to both console and file
		consoleLogger := &cleanup.StdLogger{}
		fileLogger, err := cleanup.NewFileLogger(logFile)
		if err != nil {
			log.Printf("Failed to create file logger: %v, falling back to console only", err)
			logger = consoleLogger
		} else {
			logger = cleanup.NewMultiLogger(consoleLogger, fileLogger)
		}
	} else {
		logger = &cleanup.StdLogger{} // Use simple logger without stack traces
	}

	// Create debug-aware logger if debug mode is enabled
	if debug {
		logger = &cleanup.DebugLogger{Logger: logger, DebugMode: true}
	}

	s3Handler := cleanup.NewS3Handler(logger)
	configParser := cleanup.NewYAMLConfigParser(fileReader, s3Handler, logger)
	fakeGenerator := cleanup.NewGofakeitGenerator() // Use constructor instead of &GofakeitGenerator{}
	schemaAwareGenerator := cleanup.NewSchemaAwareGofakeitGenerator(logger)
	dataCleaner := cleanup.NewDataCleanupService(dbConnector, configParser, fakeGenerator, schemaAwareGenerator, logger, workers, batchSize)
	tableFetcher := cleanup.NewMySQLTableFetcher(dbConnector, logger)

	// Create and return the service
	return &cleanup.Service{
		DBConnector:   dbConnector,
		ConfigParser:  configParser,
		DataCleaner:   dataCleaner,
		FakeGenerator: fakeGenerator,
		FileReader:    fileReader,
		Logger:        logger,
		TableFetcher:  tableFetcher,
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
	var config cleanup.Config

	flag.StringVar(&config.Host, "host", getEnvWithDefault("HOST", "localhost"), "Database host")
	flag.StringVar(&config.User, "user", getEnvWithDefault("USER", ""), "Database user")
	flag.StringVar(&config.Port, "port", getEnvWithDefault("PORT", "3306"), "Database port")
	flag.StringVar(&config.Password, "password", getEnvWithDefault("PASSWORD", ""), "Database password")
	flag.StringVar(&config.Config, "config", getEnvWithDefault("CONFIG", ""), "Configuration file path")
	flag.StringVar(&config.DB, "db", getEnvWithDefault("DB", ""), "Database name")
	// Collect multiple table flags
	var tableFlags []string
	flag.Func("table", "Table name (can be specified multiple times)", func(value string) error {
		tableFlags = append(tableFlags, value)
		return nil
	})
	flag.BoolVar(&config.AllTables, "all-tables", false, "Process all tables in the database")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug logging")
	flag.IntVar(&config.Workers, "workers", 10, "Number of worker goroutines (default: 10)")
	flag.StringVar(&config.BatchSize, "batch-size", "1K", "Batch size for updates (e.g., '1', '1K', '10K', '100K' - supports K/M/B suffixes)")
	flag.StringVar(&config.Range, "range", "", "ID range to process (e.g., '0:1000' for IDs 0-1000, '1000:' for IDs 1000+, ':100K' for IDs up to 100K, '100K:1M' for IDs 100K-1M) - colon required")
	flag.StringVar(&config.LogFile, "log-file", "", "Log file path for saving logs (optional)")

	// Add version flag
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "Show version information")

	// Parse flags
	flag.Parse()

	// Assign collected table flags to config
	config.Tables = tableFlags

	// Show version and exit if requested
	if showVersion {
		fmt.Printf("mysql-cleanup version %s\n", version)
		os.Exit(0)
	}

	// Create service with all dependencies
	service := createService(config.Debug, config.Workers, config.BatchSize, config.LogFile)
	service.Logger.Debug("Service created successfully")

	// Log command line options (with password masked)
	service.Logger.Info("Command line options:")
	service.Logger.Info(fmt.Sprintf("  Host: %s", config.Host))
	service.Logger.Info(fmt.Sprintf("  User: %s", config.User))
	service.Logger.Info(fmt.Sprintf("  Port: %s", config.Port))
	service.Logger.Info(fmt.Sprintf("  Password: %s", maskPassword(config.Password)))
	service.Logger.Info(fmt.Sprintf("  Database: %s", config.DB))
	if config.AllTables {
		service.Logger.Info("  Mode: All tables")
	} else {
		service.Logger.Info(fmt.Sprintf("  Tables: %v", config.Tables))
	}
	service.Logger.Info(fmt.Sprintf("  Workers: %d", config.Workers))
	service.Logger.Info(fmt.Sprintf("  Batch size: %s", config.BatchSize))
	service.Logger.Info(fmt.Sprintf("  Range: %s", config.Range))
	service.Logger.Info(fmt.Sprintf("  Config file: %s", config.Config))
	service.Logger.Info(fmt.Sprintf("  Log file: %s", config.LogFile))
	service.Logger.Info(fmt.Sprintf("  Debug mode: %t", config.Debug))

	// Output the provided arguments (debug mode)
	service.Logger.Debug("MySQL Cleanup CLI")
	service.Logger.Debug("==================")
	service.Logger.Debug(fmt.Sprintf("Configuration - host: %s, user: %s, port: %s, password: %s, config: %s, database: %s, range: %s",
		config.Host, config.User, config.Port, maskPassword(config.Password), config.Config, config.DB, config.Range))
	if config.AllTables {
		service.Logger.Debug("Mode: All tables")
	} else {
		service.Logger.Debug(fmt.Sprintf("Mode: Multiple tables - tables: %v", config.Tables))
	}

	// Parse and display YAML configuration if provided
	if config.Config != "" {
		service.Logger.Debug(fmt.Sprintf("Parsing YAML configuration - config_path: %s", config.Config))
		service.Logger.Debug("YAML Configuration:")
		service.Logger.Debug("===================")
		if err := service.ConfigParser.ParseAndDisplayConfigFiltered(config.Config, config); err != nil {
			service.Logger.Error(fmt.Sprintf("Error parsing config file - error: %s", err))
		}

		// Validate arguments
		service.Logger.Debug("Validating arguments")
		if config.DB == "" {
			service.Logger.Error("Error: -db argument is required")
			os.Exit(1)
		}

		if config.User == "" {
			service.Logger.Error("Error: -user argument is required")
			os.Exit(1)
		}

		if !config.AllTables && len(config.Tables) == 0 {
			service.Logger.Error("Error: Either -table (can be specified multiple times) or -all-tables argument is required")
			os.Exit(1)
		}
		service.Logger.Debug("Arguments validated successfully")

		// Perform the actual data cleanup
		service.Logger.Debug("Starting data cleanup process")
		service.Logger.Debug("Performing Data Cleanup:")
		service.Logger.Debug("========================")
		stats, err := service.DataCleaner.CleanupData(config)
		if err != nil {
			// Provide specific error messages based on failure type
			if strings.Contains(err.Error(), "connection timeout") || strings.Contains(err.Error(), "i/o timeout") {
				service.Logger.Error("Database connection timeout - server is not responding")
			} else if strings.Contains(err.Error(), "Access denied") || strings.Contains(err.Error(), "authentication") {
				service.Logger.Error("Database authentication failed - check username/password")
			} else if strings.Contains(err.Error(), "connection refused") {
				service.Logger.Error("Database connection refused - check if server is running")
			} else if strings.Contains(err.Error(), "no such host") || strings.Contains(err.Error(), "unknown host") {
				service.Logger.Error("Database host not found - check hostname/IP address")
			} else if strings.Contains(err.Error(), "Unknown database") {
				service.Logger.Error("Database not found - check database name")
			} else if strings.Contains(err.Error(), "no primary key") {
				service.Logger.Error("Table structure issue - the specified table does not have a primary key defined")
			} else {
				service.Logger.Error(fmt.Sprintf("Data cleanup failed: %s", err))
			}
			// Exit with non-zero code on any error
			os.Exit(1)
		} else {
			service.Logger.Info(fmt.Sprintf("Data cleanup completed successfully! total_rows_processed: %d, tables_processed: %d, total_duration: %s",
				stats.TotalRowsProcessed, stats.TablesProcessed, cleanup.FormatDuration(stats.TotalDuration)))
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
