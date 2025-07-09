package main

import (
	"flag"
	"log"
	"os"

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
	BatchSize int
}

// createService creates and wires up all dependencies
func createService(debug bool, workers, batchSize int) *Service {
	// Create concrete implementations
	dbConnector := &MySQLConnector{}
	fileReader := &OSFileReader{}
	logger := NewZapLogger(debug) // Use zap logger with debug flag
	configParser := NewYAMLConfigParser(fileReader, logger)
	fakeGenerator := &GofakeitGenerator{}
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
	flag.StringVar(&config.User, "user", getEnvWithDefault("USER", "root"), "Database user")
	flag.StringVar(&config.Port, "port", getEnvWithDefault("PORT", "3306"), "Database port")
	flag.StringVar(&config.Password, "password", getEnvWithDefault("PASSWORD", ""), "Database password")
	flag.StringVar(&config.Config, "config", getEnvWithDefault("CONFIG", ""), "Configuration file path")
	flag.StringVar(&config.DB, "db", getEnvWithDefault("DB", ""), "Database name")
	flag.StringVar(&config.Table, "table", getEnvWithDefault("TABLE", ""), "Table name")
	flag.BoolVar(&config.AllTables, "all-tables", false, "Process all tables in the database")
	flag.BoolVar(&config.Debug, "debug", false, "Enable debug logging")
	flag.IntVar(&config.Workers, "workers", 1, "Number of worker goroutines (default: 1)")
	flag.IntVar(&config.BatchSize, "batch-size", 1, "Batch size for updates (default: 1)")

	// Parse flags
	flag.Parse()

	// Create service with all dependencies
	service := createService(config.Debug, config.Workers, config.BatchSize)
	service.logger.Debug("Service created successfully")

	// Output the provided arguments
	service.logger.Debug("MySQL Cleanup CLI")
	service.logger.Debug("==================")
	service.logger.Debug("Configuration", 
		String("host", config.Host),
		String("user", config.User),
		String("port", config.Port),
		String("password", maskPassword(config.Password)),
		String("config", config.Config),
		String("database", config.DB),
	)
	if config.AllTables {
		service.logger.Debug("Mode: All tables")
	} else {
		service.logger.Debug("Mode: Single table", String("table", config.Table))
	}

	// Parse and display YAML configuration if provided
	if config.Config != "" {
		service.logger.Debug("Parsing YAML configuration", String("config_path", config.Config))
		service.logger.Debug("YAML Configuration:")
		service.logger.Debug("===================")
		if err := service.configParser.ParseAndDisplayConfig(config.Config); err != nil {
			service.logger.Error("Error parsing config file", Error("error", err))
		}

		// Validate arguments
		service.logger.Debug("Validating arguments")
		if config.DB == "" {
			service.logger.Error("Error: -db argument is required")
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
		if err := service.dataCleaner.CleanupData(config); err != nil {
			service.logger.Error("Error during data cleanup", Error("error", err))
		} else {
			service.logger.Info("Data cleanup completed successfully!")
		}
	}

	// Fetch and display table data if database and table are specified
	if config.DB != "" && config.Table != "" {
		service.logger.Debug("Fetching table data for display", String("database", config.DB), String("table", config.Table))
		service.logger.Debug("Table Data:")
		service.logger.Debug("===========")
		if err := service.tableFetcher.FetchAndDisplayTableData(config); err != nil {
			service.logger.Error("Error fetching table data", Error("error", err))
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
