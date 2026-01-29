package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"math/rand"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

// init seeds the random number generator to ensure unique fake data generation
func init() {
	rand.Seed(time.Now().UnixNano())
}

// FormatDuration formats a duration into a human-readable string with minutes/hours
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	} else if d < time.Hour {
		minutes := int(d.Minutes())
		seconds := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	} else {
		hours := int(d.Hours())
		minutes := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
}

// MySQLConnector implements DatabaseConnector
type MySQLConnector struct{}

func (m *MySQLConnector) Connect(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&timeout=30s&readTimeout=60s&writeTimeout=60s",
		config.User, config.Password, config.Host, config.Port, config.DB)
	// Note: We can't log the full DSN as it contains the password
	log.Printf("Connecting to MySQL database: %s@%s:%s/%s", config.User, config.Host, config.Port, config.DB)
	log.Printf("[DEBUG] DSN: %s:****@tcp(%s:%s)/%s?parseTime=true&timeout=30s&readTimeout=60s&writeTimeout=60s (config.DB=%q)", config.User, config.Host, config.Port, config.DB, config.DB)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Scale connection pool with workers
	maxConns := config.Workers + 5 // Buffer of 5 connections
	if maxConns < 20 {
		maxConns = 20 // Minimum pool size
	}

	// Set connection pool settings with longer timeouts for large datasets
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 2)

	log.Printf("Connection pool configured - max_open: %d, max_idle: %d, workers: %d", maxConns, maxConns/2, config.Workers)

	return db, nil
}

func (m *MySQLConnector) Ping(db *sql.DB) error {
	return db.Ping()
}

func (m *MySQLConnector) Close(db *sql.DB) error {
	return db.Close()
}

// YAMLConfigParser implements ConfigParser
type YAMLConfigParser struct {
	fileReader FileReader
	s3Handler  S3Handler
	logger     Logger
}

func NewYAMLConfigParser(fileReader FileReader, s3Handler S3Handler, logger Logger) *YAMLConfigParser {
	return &YAMLConfigParser{
		fileReader: fileReader,
		s3Handler:  s3Handler,
		logger:     logger,
	}
}

func (y *YAMLConfigParser) ParseConfig(configPath string) (*YAMLConfig, error) {
	var tempFilePath string
	var cleanup func()

	// S3 support: if configPath starts with s3://, download to temp file
	if strings.HasPrefix(configPath, "s3://") {
		y.logger.Info(fmt.Sprintf("Config path is S3 URI, downloading - s3_uri: %s", configPath))
		localPath, err := y.s3Handler.DownloadS3File(configPath)
		if err != nil {
			y.logger.Error(fmt.Sprintf("Failed to download config from S3 - s3_uri: %s, error: %s", configPath, err))
			return nil, err
		}
		tempFilePath = localPath
		configPath = localPath
		cleanup = func() {
			if err := y.s3Handler.CleanupTempFile(tempFilePath); err != nil {
				y.logger.Error(fmt.Sprintf("Failed to cleanup temp file - error: %s", err))
			}
		}
		defer cleanup()
		y.logger.Info(fmt.Sprintf("Downloaded S3 config to local file - local_path: %s", localPath))
	}

	y.logger.Debug(fmt.Sprintf("Reading config file - path: %s", configPath))
	data, err := y.fileReader.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	y.logger.Debug(fmt.Sprintf("YAML config parsed successfully - database_count: %d", len(yamlConfig.Databases)))
	return &yamlConfig, nil
}

func (y *YAMLConfigParser) ParseAndDisplayConfig(configPath string) error {
	yamlConfig, err := y.ParseConfig(configPath)
	if err != nil {
		return err
	}

	y.displayConfig(*yamlConfig, Config{AllTables: true}) // Show all tables for backward compatibility
	return nil
}

func (y *YAMLConfigParser) ParseAndDisplayConfigFiltered(configPath string, config Config) error {
	yamlConfig, err := y.ParseConfig(configPath)
	if err != nil {
		return err
	}

	y.displayConfig(*yamlConfig, config)
	return nil
}

func (y *YAMLConfigParser) displayConfig(yamlConfig YAMLConfig, config Config) {
	for dbName, dbConfig := range yamlConfig.Databases {
		// Only show database config if it matches target database (or show all if AllTables mode)
		if config.DB != "" && dbName != config.DB {
			continue
		}

		y.logger.Debug(fmt.Sprintf("Database configuration - database: %s, truncate_tables: %v", dbName, dbConfig.Truncate))

		if len(dbConfig.Update) > 0 {
			if config.AllTables {
				// Show all tables
				y.logger.Debug(fmt.Sprintf("Update tables configuration - database: %s, update_table_count: %d", dbName, len(dbConfig.Update)))
				for tableName, tableConfig := range dbConfig.Update {
					y.logger.Debug(fmt.Sprintf("Table configuration - database: %s, table: %s, column_count: %d", dbName, tableName, len(tableConfig.Columns)))
					if tableConfig.ExcludeClause != "" {
						y.logger.Debug(fmt.Sprintf("Table exclude clause - database: %s, table: %s, exclude_clause: %s", dbName, tableName, tableConfig.ExcludeClause))
					}
				}
			} else if len(config.Tables) > 0 {
				// Show only the target tables - check both update and truncate sections
				for _, tableName := range config.Tables {
					tableConfig, existsInUpdate := dbConfig.Update[tableName]
					existsInTruncate := false

					// Check if table exists in truncate section
					for _, truncateTable := range dbConfig.Truncate {
						if truncateTable == tableName {
							existsInTruncate = true
							break
						}
					}

					if existsInUpdate {
						y.logger.Debug(fmt.Sprintf("Update tables configuration - database: %s, target_table: %s", dbName, tableName))
						y.logger.Debug(fmt.Sprintf("Table configuration - database: %s, table: %s, column_count: %d", dbName, tableName, len(tableConfig.Columns)))
						if tableConfig.ExcludeClause != "" {
							y.logger.Debug(fmt.Sprintf("Table exclude clause - database: %s, table: %s, exclude_clause: %s", dbName, tableName, tableConfig.ExcludeClause))
						}
					}

					if existsInTruncate {
						y.logger.Debug(fmt.Sprintf("Truncate tables configuration - database: %s, target_table: %s", dbName, tableName))
					}

					if !existsInUpdate && !existsInTruncate {
						y.logger.Debug(fmt.Sprintf("Target table not found in config - database: %s, table: %s", dbName, tableName))
					}
				}
			}
		}
	}
}

// BatchProcessor handles parallel batch processing of database updates
type BatchProcessor struct {
	workerCount int
	batchSize   int
	logger      Logger
}

// BatchJob represents a batch of rows to be updated
// Add PKCols and PKValues for supporting composite PKs
type BatchJob struct {
	DatabaseName   string
	TableName      string
	PKCols         []string
	PKValues       [][]interface{} // Each row's PK values
	TableConfig    TableUpdateConfig
	ValidColumns   map[string]string // Pre-validated columns: column -> fakerType
	OrderedColumns []string          // Pre-sorted column names for consistent ordering
	BatchNumber    int               // Batch number (1, 2, 3, etc.)
	TotalBatches   int               // Total number of batches for percentage calculation
	WorkerID       int               // Worker ID that will process this batch
	RowRange       string            // Row range like "1-3" or "4-6"
}

// SampleRowData represents sample data for logging
type SampleRowData struct {
	PKValues       map[string]interface{} // Primary key values
	UpdatedColumns map[string]interface{} // Column -> new value
}

// BatchResult represents the result of processing a batch
type BatchResult struct {
	BatchJob
	ProcessedCount int
	ErrorCount     int
	Duration       time.Duration
	Errors         []error
	SampleData     []SampleRowData // First/last rows with their updated data
}

// NewBatchProcessor creates a new batch processor with optimal settings
func NewBatchProcessor(logger Logger, workers, batchSize int) *BatchProcessor {
	if workers <= 0 {
		workers = 1
	}
	if batchSize <= 0 {
		batchSize = 1
	}
	return &BatchProcessor{
		workerCount: workers,
		batchSize:   batchSize,
		logger:      logger,
	}
}

// DataCleanupService implements DataCleaner
type DataCleanupService struct {
	dbConnector          DatabaseConnector
	configParser         ConfigParser
	fakeGenerator        FakeDataGenerator
	schemaAwareGenerator SchemaAwareFakeDataGenerator
	logger               Logger
	batchProcessor       *BatchProcessor
	columnInfoCache      map[string]map[string]*ColumnInfo
	cacheMutex           sync.RWMutex
	workerBatchTimes     map[int][]time.Duration // Track batch times per worker for ETA calculation
	workerTimesMutex     sync.RWMutex            // Mutex for worker batch times
}

func NewDataCleanupService(dbConnector DatabaseConnector, configParser ConfigParser, fakeGenerator FakeDataGenerator, schemaAwareGenerator SchemaAwareFakeDataGenerator, logger Logger, workers, batchSize int) *DataCleanupService {
	return &DataCleanupService{
		dbConnector:          dbConnector,
		configParser:         configParser,
		fakeGenerator:        fakeGenerator,
		schemaAwareGenerator: schemaAwareGenerator,
		logger:               logger,
		batchProcessor:       NewBatchProcessor(logger, workers, batchSize),
		columnInfoCache:      make(map[string]map[string]*ColumnInfo),
		workerBatchTimes:     make(map[int][]time.Duration),
	}
}

func (d *DataCleanupService) CleanupData(config Config) (*CleanupStats, error) {
	startTime := time.Now()
	stats := &CleanupStats{}

	d.logger.Debug(fmt.Sprintf("Starting data cleanup - database: %s, tables: %v, all_tables: %t", config.DB, config.Tables, config.AllTables))

	// Parse the YAML configuration
	yamlConfig, err := d.configParser.ParseConfig(config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Connect to database
	d.logger.Debug("Connecting to database")
	db, err := d.dbConnector.Connect(config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer d.dbConnector.Close(db)

	// Process each database in the config
	d.logger.Debug(fmt.Sprintf("Processing databases from config - total_databases: %d", len(yamlConfig.Databases)))
	for dbName, dbConfig := range yamlConfig.Databases {
		d.logger.Debug(fmt.Sprintf("Checking database - config_db: %s, target_db: %s", dbName, config.DB))
		if dbName != config.DB {
			d.logger.Debug(fmt.Sprintf("Skipping database - database: %s, reason: not target database", dbName))
			continue // Skip if not the target database
		}

		d.logger.Debug(fmt.Sprintf("Processing database - database: %s", dbName))

		// Handle truncate tables - only in all tables mode
		if config.AllTables {
			d.logger.Debug(fmt.Sprintf("Checking truncate tables - truncate_count: %d", len(dbConfig.Truncate)))
			if len(dbConfig.Truncate) > 0 {
				if err := d.TruncateTables(db, dbConfig.Truncate); err != nil {
					d.logger.Warn(fmt.Sprintf("Failed to truncate tables - error: %s", err))
				}
			}
		} else {
			d.logger.Debug("Skipping truncate tables - single table mode")
		}

		// Handle update tables
		if len(dbConfig.Update) > 0 {
			if config.AllTables {
				// Process all tables
				d.logger.Debug(fmt.Sprintf("Checking update tables - update_count: %d", len(dbConfig.Update)))
				d.logger.Debug("Processing all tables mode")
				d.logger.Debug(fmt.Sprintf("Processing all tables - table_count: %d", len(dbConfig.Update)))
				tableStats, err := d.UpdateTables(db, config.DB, dbConfig.Update, config)
				if err != nil {
					d.logger.Error(fmt.Sprintf("Failed to update tables - error: %s", err))
					return nil, err
				}
				stats.TotalRowsProcessed += tableStats.TotalRowsProcessed
				stats.TablesProcessed += tableStats.TablesProcessed
			} else {
				// Process multiple specified tables
				d.logger.Debug(fmt.Sprintf("Processing multiple tables - tables: %v", config.Tables))

				// Track which tables were found and processed
				foundTables := make(map[string]bool)
				processedTables := 0

				for _, tableName := range config.Tables {
					d.logger.Debug(fmt.Sprintf("Processing table - table: %s", tableName))

					// Check if table exists in update section
					tableConfig, existsInUpdate := dbConfig.Update[tableName]
					existsInTruncate := false

					// Check if table exists in truncate section
					for _, truncateTable := range dbConfig.Truncate {
						if truncateTable == tableName {
							existsInTruncate = true
							break
						}
					}

					if !existsInUpdate && !existsInTruncate {
						d.logger.Debug(fmt.Sprintf("Table not found in config - table: %s, available_update_tables: %v, available_truncate_tables: %v",
							tableName, getKeys(dbConfig.Update), dbConfig.Truncate))
						return nil, fmt.Errorf("table '%s' not found in configuration (checked both update and truncate sections)", tableName)
					}

					// Process truncate if table is in truncate section
					if existsInTruncate {
						d.logger.Debug(fmt.Sprintf("Truncating table - table: %s", tableName))
						if err := d.TruncateTables(db, []string{tableName}); err != nil {
							d.logger.Error(fmt.Sprintf("Failed to truncate table - table: %s, error: %s", tableName, err))
							return nil, err
						}
						processedTables++
						foundTables[tableName] = true
					}

					// Process update if table is in update section
					if existsInUpdate {
						d.logger.Debug(fmt.Sprintf("Processing table update - table: %s", tableName))
						rowsProcessed, err := d.UpdateTableData(db, config.DB, tableName, tableConfig, config)
						if err != nil {
							return nil, err
						}
						stats.TotalRowsProcessed += rowsProcessed
						processedTables++
						foundTables[tableName] = true
					}
				}

				stats.TablesProcessed = processedTables
				d.logger.Debug(fmt.Sprintf("Completed processing multiple tables - processed_count: %d, tables: %v", processedTables, config.Tables))
			}
		}
	}

	stats.TotalDuration = time.Since(startTime)

	// Log overall completion at info level
	d.logger.Info(fmt.Sprintf("Data cleanup completed - total_processed: %d, tables_processed: %d, duration: %s",
		stats.TotalRowsProcessed, stats.TablesProcessed, FormatDuration(stats.TotalDuration)))

	return stats, nil
}

// getKeys returns the keys of a map as a slice
func getKeys(m map[string]TableUpdateConfig) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (d *DataCleanupService) TruncateTables(db *sql.DB, tables []string) error {
	for _, table := range tables {
		d.logger.Debug(fmt.Sprintf("Truncating table - table: %s", table))
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		if _, err := db.Exec(query); err != nil {
			d.logger.Error(fmt.Sprintf("Failed to truncate table - table: %s, error: %s", table, err))
			return fmt.Errorf("failed to truncate table %s: %w", table, err)
		}
		d.logger.Debug(fmt.Sprintf("Successfully truncated table - table: %s", table))
	}
	return nil
}

func (d *DataCleanupService) UpdateTables(db *sql.DB, databaseName string, tableConfigs map[string]TableUpdateConfig, config Config) (*CleanupStats, error) {
	stats := &CleanupStats{}

	for tableName, tableConfig := range tableConfigs {
		d.logger.Debug(fmt.Sprintf("Updating table - table: %s", tableName))

		rowsProcessed, err := d.UpdateTableData(db, databaseName, tableName, tableConfig, config)
		if err != nil {
			return nil, fmt.Errorf("failed to update table %s: %w", tableName, err)
		}

		stats.TotalRowsProcessed += rowsProcessed
		stats.TablesProcessed++

		d.logger.Debug(fmt.Sprintf("Successfully updated table - table: %s", tableName))
	}
	return stats, nil
}

func (d *DataCleanupService) UpdateTableData(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig, config Config) (int, error) {
	// Parse ID range if specified
	idRange, err := ParseIDRange(config.Range)
	if err != nil {
		return 0, fmt.Errorf("invalid range specification: %w", err)
	}

	d.logger.Debug(fmt.Sprintf("Starting parallel table data update - table: %s, worker_count: %d, batch_size: %d, range: %s",
		tableName, d.batchProcessor.workerCount, d.batchProcessor.batchSize, idRange.String()))

	startTime := time.Now()

	// Discover primary key columns
	pkCols, err := getPrimaryKeyColumns(db, databaseName, tableName, d.logger)
	if err != nil {
		// Check if it's a "no primary key" error
		if strings.Contains(err.Error(), "has no primary key defined") {
			d.logger.Info(fmt.Sprintf("Table has no primary key, using offset-based processing - table: %s", tableName))

			// Check if range filtering is requested (not supported for offset-based processing)
			if idRange.HasRange {
				return 0, fmt.Errorf("range filtering is not supported for tables without primary keys")
			}

			// Use offset-based processing for tables without primary keys
			return d.processTableWithoutPrimaryKey(db, databaseName, tableName, tableConfig, idRange)
		}
		return 0, fmt.Errorf("failed to discover primary key: %w", err)
	}

	// Check if range filtering is possible (requires single-column PK)
	if idRange.HasRange && len(pkCols) != 1 {
		return 0, fmt.Errorf("range filtering requires a single-column primary key, but table %s has %d primary key columns", tableName, len(pkCols))
	}

	// Pre-cache column info to avoid concurrent map access issues
	d.cacheMutex.Lock()
	if _, exists := d.columnInfoCache[tableName]; !exists {
		d.columnInfoCache[tableName] = make(map[string]*ColumnInfo)
	}
	d.cacheMutex.Unlock()

	for columnName := range tableConfig.Columns {
		info, err := d.schemaAwareGenerator.GetColumnInfo(databaseName, tableName, columnName, db)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to pre-cache column info - table: %s, column: %s, error: %s", tableName, columnName, err))
			continue
		}
		d.cacheMutex.Lock()
		d.columnInfoCache[tableName][columnName] = info
		d.cacheMutex.Unlock()
	}

	// Validate columns before starting the parallel processing
	validColumns := make(map[string]string)
	for col, fakerType := range tableConfig.Columns {
		exists, err := d.columnExists(db, databaseName, tableName, col)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to check column - table: %s, column: %s, error: %s", tableName, col, err))
			continue
		}
		if !exists {
			d.logger.Warn(fmt.Sprintf("Column does not exist, skipping - table: %s, column: %s", tableName, col))
			continue
		}
		validColumns[col] = fakerType
	}

	if len(validColumns) == 0 {
		d.logger.Warn(fmt.Sprintf("No valid columns found for update - table: %s", tableName))
		return 0, nil
	}

	d.logger.Debug(fmt.Sprintf("Pre-validated columns - table: %s, valid_columns: %d", tableName, len(validColumns)))

	// Process the table with optional range filtering
	totalProcessed, err := d.processTableWithRange(db, databaseName, tableName, tableConfig, pkCols, validColumns, idRange)
	if err != nil {
		return 0, err
	}

	duration := time.Since(startTime)
	d.logger.Info(fmt.Sprintf("Table processing completed - table: %s, processed: %d, range: %s, duration: %s, rows_per_second: %.1f",
		tableName, totalProcessed, idRange.String(), FormatDuration(duration), float64(totalProcessed)/duration.Seconds()))

	return totalProcessed, nil
}

// processTableWithOffset processes a table without primary key using LIMIT/OFFSET
func (d *DataCleanupService) processTableWithOffset(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig, validColumns map[string]string, idRange *IDRange) (int, error) {
	// Build WHERE clause with exclude conditions (no range filtering for offset-based processing)
	var whereConditions []string

	// Add exclude clause if specified
	if tableConfig.ExcludeClause != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("NOT (%s)", tableConfig.ExcludeClause))
		d.logger.Debug(fmt.Sprintf("Using exclude clause - exclude_clause: %s", tableConfig.ExcludeClause))
	}

	// Build final WHERE clause
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	// Get total row count for progress tracking
	totalRows, err := getTableRowCount(db, databaseName, tableName, whereClause)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err)
	}

	if totalRows == 0 {
		d.logger.Info(fmt.Sprintf("Table processing completed - table: %s, no rows to update", tableName))
		return 0, nil
	}

	d.logger.Info(fmt.Sprintf("Starting offset-based processing - table: %s, total_rows: %d, workers: %d, batch_size: %d",
		tableName, totalRows, d.batchProcessor.workerCount, d.batchProcessor.batchSize))

	// Calculate total batches
	totalBatches := (totalRows + d.batchProcessor.batchSize - 1) / d.batchProcessor.batchSize

	// Process batches in parallel using offset
	totalProcessed, totalErrors, batchErr := d.processBatchesInParallelOffset(db, databaseName, tableName, tableConfig, validColumns, whereClause, totalRows, totalBatches)

	if batchErr != nil {
		return 0, batchErr
	}

	if totalErrors > 0 {
		d.logger.Warn(fmt.Sprintf("Table processing had errors - table: %s, processed: %d, errors: %d",
			tableName, totalProcessed, totalErrors))
	}

	d.logger.Info(fmt.Sprintf("Offset-based processing completed - table: %s, processed: %d, errors: %d",
		tableName, totalProcessed, totalErrors))

	return totalProcessed, nil
}

// processBatchesInParallelOffset processes row batches using LIMIT/OFFSET (for tables without PK)
func (d *DataCleanupService) processBatchesInParallelOffset(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig, validColumns map[string]string, whereClause string, totalRows, totalBatches int) (int, int, error) {
	// Reset worker batch times for fresh ETA calculation
	d.resetWorkerBatchTimes()

	jobChan := make(chan BatchJob, totalBatches)
	resultChan := make(chan BatchResult, totalBatches)

	// Create context for cancellation when errors occur
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < d.batchProcessor.workerCount; i++ {
		wg.Add(1)
		uniqueSeed := time.Now().UnixNano() + int64(i*1000000) + rand.Int63()
		workerGenerator := NewSchemaAwareGofakeitGenerator(d.logger)
		workerGenerator.faker = gofakeit.New(uniqueSeed)
		go d.workerOffsetWithContext(ctx, i, db, workerGenerator, jobChan, resultChan, &wg)
		time.Sleep(time.Microsecond)
	}

	go func() {
		defer close(jobChan)
		for batchNum := 0; batchNum < totalBatches; batchNum++ {
			offset := batchNum * d.batchProcessor.batchSize
			limit := d.batchProcessor.batchSize
			if offset+limit > totalRows {
				limit = totalRows - offset
			}

			job := BatchJob{
				DatabaseName: databaseName,
				TableName:    tableName,
				TableConfig:  tableConfig,
				ValidColumns: validColumns,
				BatchNumber:  batchNum + 1,
				TotalBatches: totalBatches,
				WorkerID:     batchNum % d.batchProcessor.workerCount,
				RowRange:     fmt.Sprintf("%d-%d", offset+1, offset+limit),
			}

			select {
			case jobChan <- job:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	var totalProcessed, totalErrors int
	var errors []error

	for result := range resultChan {
		totalProcessed += result.ProcessedCount
		totalErrors += result.ErrorCount
		errors = append(errors, result.Errors...)

		// Log progress
		if result.BatchNumber%10 == 0 || result.BatchNumber == result.TotalBatches {
			eta := d.calculateETA(result.WorkerID, result.BatchNumber, result.TotalBatches)
			d.logger.Info(fmt.Sprintf("Batch progress - table: %s, batch: %d/%d, processed: %d, errors: %d, eta: %s",
				tableName, result.BatchNumber, result.TotalBatches, totalProcessed, totalErrors, eta))
		}

		// Record batch time for ETA calculation
		d.recordBatchTime(result.WorkerID, result.Duration)
	}

	if len(errors) > 0 {
		d.logger.Warn(fmt.Sprintf("Batch processing errors - table: %s, error_count: %d", tableName, len(errors)))
		for _, err := range errors {
			d.logger.Warn(fmt.Sprintf("Batch error - table: %s, error: %s", tableName, err))
		}
	}

	return totalProcessed, totalErrors, nil
}

// workerOffsetWithContext processes offset-based batches
func (d *DataCleanupService) workerOffsetWithContext(ctx context.Context, workerID int, db *sql.DB, generator *SchemaAwareGofakeitGenerator, jobChan <-chan BatchJob, resultChan chan<- BatchResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			processed, errors, sampleData, err := d.processBatchOffset(db, generator, job)
			result := BatchResult{
				BatchJob:       job,
				ProcessedCount: processed,
				ErrorCount:     errors,
				Duration:       time.Since(time.Now()), // Will be set properly in processBatchOffset
				Errors:         []error{err},
				SampleData:     sampleData,
			}
			if err != nil {
				result.Errors = append(result.Errors, err)
			}
			select {
			case resultChan <- result:
			case <-ctx.Done():
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// processBatchOffset processes a single batch using LIMIT/OFFSET
func (d *DataCleanupService) processBatchOffset(db *sql.DB, generator *SchemaAwareGofakeitGenerator, job BatchJob) (int, int, []SampleRowData, error) {
	startTime := time.Now()

	// Parse offset and limit from RowRange
	parts := strings.Split(job.RowRange, "-")
	if len(parts) != 2 {
		return 0, 0, nil, fmt.Errorf("invalid row range format: %s", job.RowRange)
	}

	startRow, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid start row: %s", parts[0])
	}

	endRow, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, nil, fmt.Errorf("invalid end row: %s", parts[1])
	}

	offset := startRow - 1
	limit := endRow - startRow + 1

	// Build WHERE clause
	whereClause := ""
	if job.TableConfig.ExcludeClause != "" {
		whereClause = fmt.Sprintf("WHERE NOT (%s)", job.TableConfig.ExcludeClause)
	}

	// Build UPDATE query with LIMIT/OFFSET
	updates := make([]string, 0)
	args := make([]interface{}, 0)

	for column, fakerType := range job.ValidColumns {
		// Check if column exists
		exists, err := d.columnExists(db, job.DatabaseName, job.TableName, column)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to check column - table: %s, column: %s, error: %s", job.TableName, column, err))
			continue
		}
		if !exists {
			d.logger.Warn(fmt.Sprintf("Column does not exist, skipping - table: %s, column: %s", job.TableName, column))
			continue
		}

		// Get column info
		d.cacheMutex.RLock()
		info := d.columnInfoCache[job.TableName][column]
		d.cacheMutex.RUnlock()

		if info == nil {
			// Fetch column info if not cached
			info, err = generator.GetColumnInfo(job.DatabaseName, job.TableName, column, db)
			if err != nil {
				d.logger.Warn(fmt.Sprintf("Failed to get column info - table: %s, column: %s, error: %s", job.TableName, column, err))
				continue
			}
			// Cache the info
			d.cacheMutex.Lock()
			if d.columnInfoCache[job.TableName] == nil {
				d.columnInfoCache[job.TableName] = make(map[string]*ColumnInfo)
			}
			d.columnInfoCache[job.TableName][column] = info
			d.cacheMutex.Unlock()
		}

		// Generate fake value
		fakeValue, err := generator.GenerateFakeValue(fakerType, info)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to generate fake value - table: %s, column: %s, faker_type: %s, error: %s", job.TableName, column, fakerType, err))
			continue
		}

		updates = append(updates, fmt.Sprintf("`%s` = ?", column))
		args = append(args, fakeValue)
	}

	if len(updates) == 0 {
		return 0, 0, nil, nil
	}

	// Build the UPDATE query
	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s %s LIMIT %d OFFSET %d",
		job.DatabaseName, job.TableName, strings.Join(updates, ", "), whereClause, limit, offset)

	d.logger.Debug(fmt.Sprintf("Executing offset-based update - table: %s, batch: %d, offset: %d, limit: %d, query: %s",
		job.TableName, job.BatchNumber, offset, limit, query))

	// Execute the UPDATE query
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, 1, nil, fmt.Errorf("failed to execute offset-based UPDATE query: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	duration := time.Since(startTime)

	d.logger.Debug(fmt.Sprintf("Offset-based batch completed - table: %s, batch: %d, rows_affected: %d, duration: %s",
		job.TableName, job.BatchNumber, rowsAffected, FormatDuration(duration)))

	return int(rowsAffected), 0, nil, nil
}

// processTableWithoutPrimaryKey handles tables without primary keys using offset-based processing
func (d *DataCleanupService) processTableWithoutPrimaryKey(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig, idRange *IDRange) (int, error) {
	// Pre-cache column info to avoid concurrent map access issues
	d.cacheMutex.Lock()
	if _, exists := d.columnInfoCache[tableName]; !exists {
		d.columnInfoCache[tableName] = make(map[string]*ColumnInfo)
	}
	d.cacheMutex.Unlock()

	for columnName := range tableConfig.Columns {
		info, err := d.schemaAwareGenerator.GetColumnInfo(databaseName, tableName, columnName, db)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to pre-cache column info - table: %s, column: %s, error: %s", tableName, columnName, err))
			continue
		}
		d.cacheMutex.Lock()
		d.columnInfoCache[tableName][columnName] = info
		d.cacheMutex.Unlock()
	}

	// Validate columns before starting the parallel processing
	validColumns := make(map[string]string)
	for col, fakerType := range tableConfig.Columns {
		exists, err := d.columnExists(db, databaseName, tableName, col)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to check column - table: %s, column: %s, error: %s", tableName, col, err))
			continue
		}
		if !exists {
			d.logger.Warn(fmt.Sprintf("Column does not exist, skipping - table: %s, column: %s", tableName, col))
			continue
		}
		validColumns[col] = fakerType
	}

	if len(validColumns) == 0 {
		d.logger.Warn(fmt.Sprintf("No valid columns found for update - table: %s", tableName))
		return 0, nil
	}

	d.logger.Debug(fmt.Sprintf("Pre-validated columns - table: %s, valid_columns: %d", tableName, len(validColumns)))

	// Process the table using offset-based approach
	totalProcessed, err := d.processTableWithOffset(db, databaseName, tableName, tableConfig, validColumns, idRange)
	if err != nil {
		return 0, err
	}

	return totalProcessed, nil
}

// processBatchesInParallelPK processes row batches using a worker pool (PK-aware)
func (d *DataCleanupService) processBatchesInParallelPK(db *sql.DB, databaseName, tableName string, pkCols []string, pkValues [][]interface{}, tableConfig TableUpdateConfig, validColumns map[string]string) (int, int, error) {
	// Reset worker batch times for fresh ETA calculation
	d.resetWorkerBatchTimes()

	// Pre-compute ordered columns once for all batches
	orderedColumns := make([]string, 0, len(validColumns))
	for col := range validColumns {
		orderedColumns = append(orderedColumns, col)
	}
	sort.Strings(orderedColumns)

	batches := d.createBatchesPK(pkValues, d.batchProcessor.batchSize)
	jobChan := make(chan BatchJob, len(batches))
	resultChan := make(chan BatchResult, len(batches))

	// Create context for cancellation when errors occur
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < d.batchProcessor.workerCount; i++ {
		wg.Add(1)
		uniqueSeed := time.Now().UnixNano() + int64(i*1000000) + rand.Int63()
		workerGenerator := NewSchemaAwareGofakeitGenerator(d.logger)
		workerGenerator.faker = gofakeit.New(uniqueSeed)
		go d.workerPKWithContext(ctx, i, db, workerGenerator, jobChan, resultChan, &wg)
		time.Sleep(time.Microsecond)
	}

	go func() {
		defer close(jobChan)
		for batchIndex, batch := range batches {
			select {
			case <-ctx.Done():
				return
			default:
				rowRange := "empty"
				if len(batch) > 0 {
					firstPK := batch[0][0]
					lastPK := batch[len(batch)-1][0]
					if len(batch) == 1 {
						rowRange = fmt.Sprintf("%v", firstPK)
					} else {
						rowRange = fmt.Sprintf("%v-%v", firstPK, lastPK)
					}
				}
				select {
				case jobChan <- BatchJob{
					DatabaseName:   databaseName,
					TableName:      tableName,
					PKCols:         pkCols,
					PKValues:       batch,
					TableConfig:    tableConfig,
					ValidColumns:   validColumns,
					OrderedColumns: orderedColumns,
					BatchNumber:    batchIndex + 1,
					TotalBatches:   len(batches),
					WorkerID:       -1,
					RowRange:       rowRange,
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	totalProcessed := 0
	totalErrors := 0
	totalBatches := len(batches)
	completedBatches := 0
	totalBatchDuration := time.Duration(0)
	tableStartTime := time.Now() // Track actual wall clock time

	for result := range resultChan {
		totalProcessed += result.ProcessedCount
		totalErrors += result.ErrorCount
		completedBatches++
		totalBatchDuration += result.Duration

		if len(result.Errors) > 0 {
			cancel()
			d.logger.Error(fmt.Sprintf("Database error detected - cancelling all workers and failing operation - batch: %d, error: %v", result.BatchNumber, result.Errors[0]))
			return totalProcessed, totalErrors, fmt.Errorf("database error in batch %d: %v", result.BatchNumber, result.Errors[0])
		}

		// Log sample data at debug level
		if len(result.SampleData) > 0 {
			d.logger.Debug(fmt.Sprintf("Batch %d/%d sample data - table: %s, sample_rows: %s",
				result.BatchNumber, totalBatches, result.TableName, d.formatSampleData(result.SampleData)))
		}

		// Global percent and ETA with improved calculation
		globalPercent := float64(completedBatches) / float64(totalBatches) * 100

		// Track individual batch times for better ETA calculation
		d.recordBatchTime(0, result.Duration) // Use worker 0 as global tracker

		// Calculate ETA based on actual wall clock time (not summed batch times)
		globalETA := time.Duration(0)
		if completedBatches > 0 && globalPercent > 0.1 {
			// Use actual wall clock time instead of summed batch times
			wallClockTime := time.Since(tableStartTime).Seconds()
			if wallClockTime > 0 && globalPercent > 0 {
				// Time per 1% = wall clock time / current percentage
				timePerPercent := wallClockTime / globalPercent

				// Remaining percentage
				remainingPercent := 100.0 - globalPercent

				// ETA = time per percent × remaining percent
				etaSeconds := timePerPercent * remainingPercent
				globalETA = time.Duration(etaSeconds * float64(time.Second))

				// Debug logging
				d.logger.Debug(fmt.Sprintf("ETA Debug - wall_clock_time: %.1fs, global_percent: %.1f%%, time_per_percent: %.1fs, remaining_percent: %.1f%%, calculated_eta: %s",
					wallClockTime, globalPercent, timePerPercent, remainingPercent, FormatDuration(globalETA)))
			}
		}

		// Log global progress every batch (or every N batches if you want less spam)
		d.logger.Info(fmt.Sprintf("Table progress - table: %s, completed_batches: %d/%d, percent: %.1f%%, ETA: %s",
			result.TableName, completedBatches, totalBatches, globalPercent, FormatDuration(globalETA)))
	}

	d.logger.Info(fmt.Sprintf("Batch processing completed - table: %s, total_processed: %d, total_errors: %d, total_batches: %d",
		tableName, totalProcessed, totalErrors, totalBatches))

	return totalProcessed, totalErrors, nil
}

// createBatchesPK splits PK value slices into batches
func (d *DataCleanupService) createBatchesPK(pkValues [][]interface{}, batchSize int) [][][]interface{} {
	var batches [][][]interface{}
	for i := 0; i < len(pkValues); i += batchSize {
		end := i + batchSize
		if end > len(pkValues) {
			end = len(pkValues)
		}
		batches = append(batches, pkValues[i:end])
	}
	return batches
}

// workerPKWithContext processes PK-aware batch jobs with context cancellation
func (d *DataCleanupService) workerPKWithContext(ctx context.Context, workerID int, db *sql.DB, generator *SchemaAwareGofakeitGenerator, jobChan <-chan BatchJob, resultChan chan<- BatchResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return // Channel closed, exit
			}
			// Set the worker ID on the job
			job.WorkerID = workerID

			startTime := time.Now()
			processed, errors, sampleData, batchErr := d.processBatchPK(db, generator, job)
			duration := time.Since(startTime)

			// Create result with errors if any occurred
			var errorsList []error
			if batchErr != nil {
				errorsList = []error{batchErr}
			}

			resultChan <- BatchResult{
				BatchJob:       job,
				ProcessedCount: processed,
				ErrorCount:     errors,
				Duration:       duration,
				Errors:         errorsList,
				SampleData:     sampleData,
			}
		case <-ctx.Done():
			return // Context cancelled, exit immediately
		}
	}
}

// workerPK processes PK-aware batch jobs
func (d *DataCleanupService) workerPK(workerID int, db *sql.DB, generator *SchemaAwareGofakeitGenerator, jobChan <-chan BatchJob, resultChan chan<- BatchResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// Set the worker ID on the job
		job.WorkerID = workerID

		startTime := time.Now()
		processed, errors, sampleData, batchErr := d.processBatchPK(db, generator, job)
		duration := time.Since(startTime)

		// Create result with errors if any occurred
		var errorsList []error
		if batchErr != nil {
			errorsList = []error{batchErr}
		}

		resultChan <- BatchResult{
			BatchJob:       job,
			ProcessedCount: processed,
			ErrorCount:     errors,
			Duration:       duration,
			Errors:         errorsList,
			SampleData:     sampleData,
		}
	}
}

// processBatchPK processes a single batch of rows using bulk UPDATE with CASE statements
func (d *DataCleanupService) processBatchPK(db *sql.DB, generator *SchemaAwareGofakeitGenerator, job BatchJob) (int, int, []SampleRowData, error) {
	if len(job.PKValues) == 0 {
		return 0, 0, nil, nil
	}

	d.logger.Info(fmt.Sprintf("Worker %d batch %d/%d starting - table: %s, row_range: %s, batch_size: %d",
		job.WorkerID, job.BatchNumber, job.TotalBatches, job.TableName, job.RowRange, len(job.PKValues)))

	// Generate fake values for all rows in the batch first
	type RowUpdate struct {
		PKValues      []interface{}
		ColumnUpdates map[string]interface{}
	}

	rowUpdates := make([]RowUpdate, 0, len(job.PKValues))

	// Copy column info to local map before the row loop - single lock acquisition
	d.cacheMutex.RLock()
	localColumnInfo := make(map[string]*ColumnInfo, len(job.ValidColumns))
	for col := range job.ValidColumns {
		localColumnInfo[col] = d.columnInfoCache[job.TableName][col]
	}
	d.cacheMutex.RUnlock()

	for _, pkVals := range job.PKValues {
		columnUpdates := make(map[string]interface{}, len(job.ValidColumns))
		for column, fakerType := range job.ValidColumns {
			// Get column info from local cache - no lock needed
			info := localColumnInfo[column]

			fakeValue, err := generator.GenerateFakeValue(fakerType, info)
			if err != nil {
				d.logger.Warn(fmt.Sprintf("Failed to generate fake value - table: %s, column: %s, faker_type: %s, error: %s", job.TableName, column, fakerType, err))
				continue
			}
			columnUpdates[column] = fakeValue
		}
		if len(columnUpdates) > 0 {
			rowUpdates = append(rowUpdates, RowUpdate{
				PKValues:      pkVals,
				ColumnUpdates: columnUpdates,
			})
		}
	}

	if len(rowUpdates) == 0 {
		return 0, 0, nil, nil
	}

	// Build UPDATE query similar to Python implementation
	// Format: UPDATE table SET col1=?, col2=? WHERE pk=?

	// Use pre-computed ordered columns from job (already sorted)
	orderedColumns := job.OrderedColumns

	setClauses := make([]string, 0, len(orderedColumns))
	for _, column := range orderedColumns {
		setClauses = append(setClauses, fmt.Sprintf("`%s` = ?", column))
	}

	// Build WHERE clause for primary key
	whereClauses := make([]string, 0, len(job.PKCols))
	for _, pkCol := range job.PKCols {
		whereClauses = append(whereClauses, fmt.Sprintf("`%s` = ?", pkCol))
	}

	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s",
		job.DatabaseName, job.TableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	// Add exclude clause if specified
	if job.TableConfig.ExcludeClause != "" {
		query += fmt.Sprintf(" AND NOT (%s)", job.TableConfig.ExcludeClause)
	}

	// Prepare the statement for executemany
	stmt, err := db.Prepare(query)
	if err != nil {
		d.logger.Error(fmt.Sprintf("Failed to prepare statement - table: %s, error: %s", job.TableName, err))
		return 0, len(rowUpdates), nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Build arguments for executemany (similar to Python's executemany)
	args := make([][]interface{}, 0, len(rowUpdates))
	for _, rowUpdate := range rowUpdates {
		rowArgs := make([]interface{}, 0, len(orderedColumns)+len(job.PKCols))

		// Add SET values first (column values) in the same order as SET clause
		for _, column := range orderedColumns {
			if val, exists := rowUpdate.ColumnUpdates[column]; exists {
				rowArgs = append(rowArgs, val)
			} else {
				rowArgs = append(rowArgs, nil)
			}
		}

		// Add WHERE values (PK values)
		rowArgs = append(rowArgs, rowUpdate.PKValues...)

		args = append(args, rowArgs)
	}

	// Execute all updates in a transaction for better performance
	tx, err := db.Begin()
	if err != nil {
		d.logger.Error(fmt.Sprintf("Failed to begin transaction - table: %s, error: %s", job.TableName, err))
		return 0, len(rowUpdates), nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Use prepared statement within transaction
	txStmt := tx.Stmt(stmt)
	defer txStmt.Close()

	var totalRowsAffected int64
	for _, rowArgs := range args {
		result, err := txStmt.Exec(rowArgs...)
		if err != nil {
			tx.Rollback()
			d.logger.Error(fmt.Sprintf("Failed to execute update - table: %s, error: %s", job.TableName, err))
			return 0, len(rowUpdates), nil, fmt.Errorf("database error: %w", err)
		}
		rowsAffected, _ := result.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		d.logger.Error(fmt.Sprintf("Failed to commit transaction - table: %s, error: %s", job.TableName, err))
		return 0, len(rowUpdates), nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Collect sample data for debug logging (only the last row)
	var sampleData []SampleRowData
	if len(rowUpdates) > 0 {
		lastRow := rowUpdates[len(rowUpdates)-1]
		pkValues := make(map[string]interface{})
		for j, col := range job.PKCols {
			pkValues[col] = lastRow.PKValues[j]
		}
		sampleData = append(sampleData, SampleRowData{
			PKValues:       pkValues,
			UpdatedColumns: lastRow.ColumnUpdates,
		})
	}

	return int(totalRowsAffected), 0, sampleData, nil
}

// formatSampleData formats sample row data for debug logging
func (d *DataCleanupService) formatSampleData(sampleData []SampleRowData) string {
	if len(sampleData) == 0 {
		return "no sample data"
	}

	// Only show the last sample row
	sample := sampleData[len(sampleData)-1]

	// Use strings.Builder for efficient string concatenation
	var pkBuf strings.Builder
	first := true
	for col, val := range sample.PKValues {
		if !first {
			pkBuf.WriteString(", ")
		}
		first = false
		fmt.Fprintf(&pkBuf, "%s:%v", col, val)
	}

	var updateBuf strings.Builder
	first = true
	for col, val := range sample.UpdatedColumns {
		if !first {
			updateBuf.WriteString(", ")
		}
		first = false
		// Show complete values to verify uniqueness
		fmt.Fprintf(&updateBuf, "%s:'%v'", col, val)
	}

	return fmt.Sprintf("[PK:{%s} -> {%s}]", pkBuf.String(), updateBuf.String())
}

// recordBatchTime records a batch completion time for a worker
func (d *DataCleanupService) recordBatchTime(workerID int, duration time.Duration) {
	d.workerTimesMutex.Lock()
	defer d.workerTimesMutex.Unlock()

	if d.workerBatchTimes[workerID] == nil {
		d.workerBatchTimes[workerID] = make([]time.Duration, 0)
	}

	// Keep only the last 10 batch times for moving average (avoid memory growth)
	if len(d.workerBatchTimes[workerID]) >= 10 {
		d.workerBatchTimes[workerID] = d.workerBatchTimes[workerID][1:]
	}

	d.workerBatchTimes[workerID] = append(d.workerBatchTimes[workerID], duration)
}

// calculateETA calculates estimated time to completion for a worker
func (d *DataCleanupService) calculateETA(workerID int, batchNumber, totalBatches int) string {
	d.workerTimesMutex.RLock()
	defer d.workerTimesMutex.RUnlock()

	batchTimes := d.workerBatchTimes[workerID]
	if len(batchTimes) == 0 {
		return "ETA: calculating..."
	}

	// Calculate average batch time for this worker
	var totalTime time.Duration
	for _, duration := range batchTimes {
		totalTime += duration
	}
	avgBatchTime := totalTime / time.Duration(len(batchTimes))

	// Estimate remaining batches (simple approximation: divide remaining batches among workers)
	remainingBatches := totalBatches - batchNumber
	if remainingBatches <= 0 {
		return "ETA: complete"
	}

	// Estimate this worker will handle roughly 1/workerCount of remaining batches
	workerCount := d.batchProcessor.workerCount
	estimatedRemainingForWorker := (remainingBatches + workerCount - 1) / workerCount // ceiling division

	estimatedTimeRemaining := avgBatchTime * time.Duration(estimatedRemainingForWorker)

	return fmt.Sprintf("ETA: %s", FormatDuration(estimatedTimeRemaining))
}

// resetWorkerBatchTimes clears batch time history for all workers (called at start of table processing)
func (d *DataCleanupService) resetWorkerBatchTimes() {
	d.workerTimesMutex.Lock()
	defer d.workerTimesMutex.Unlock()
	d.workerBatchTimes = make(map[int][]time.Duration)
}

func (d *DataCleanupService) updateSingleRow(db *sql.DB, databaseName, tableName string, rowID int64, tableConfig TableUpdateConfig) error {
	d.logger.Debug(fmt.Sprintf("Updating single row - table: %s, row_id: %d", tableName, rowID))

	// Discover primary key columns
	pkCols, err := getPrimaryKeyColumns(db, databaseName, tableName, d.logger)
	if err != nil {
		return fmt.Errorf("failed to discover primary key: %w", err)
	}
	if len(pkCols) != 1 {
		return fmt.Errorf("single-row update only supports single-column PKs (got %d)", len(pkCols))
	}

	// Build the UPDATE query for this specific row
	updates := make([]string, 0)
	args := make([]interface{}, 0)

	for column, fakerType := range tableConfig.Columns {
		// Check if column exists before trying to update it
		exists, err := d.columnExists(db, databaseName, tableName, column)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to check column - table: %s, column: %s, error: %s", tableName, column, err))
			continue
		}
		if !exists {
			d.logger.Warn(fmt.Sprintf("Column does not exist, skipping - table: %s, column: %s", tableName, column))
			continue
		}

		d.cacheMutex.RLock()
		info := d.columnInfoCache[tableName][column]
		d.cacheMutex.RUnlock()

		fakeValue, err := d.schemaAwareGenerator.GenerateFakeValue(fakerType, info)
		if err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to generate fake value for single row update - table: %s, column: %s, faker_type: %s, error: %s", tableName, column, fakerType, err))
			continue
		}
		updates = append(updates, fmt.Sprintf("`%s` = ?", column))
		args = append(args, fakeValue)
	}

	if len(updates) == 0 {
		return nil
	}

	// Add the PK value to the args
	args = append(args, rowID)

	// Execute the UPDATE query for this specific row
	query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE `%s` = ?", databaseName, tableName, strings.Join(updates, ", "), pkCols[0])
	d.logger.Debug(fmt.Sprintf("Executing update query - table: %s, row_id: %d, query: %s", tableName, rowID, query))

	result, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute UPDATE query for row %d: %w", rowID, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		d.logger.Warn(fmt.Sprintf("No rows affected - table: %s, row_id: %d", tableName, rowID))
	}

	return nil
}

func (d *DataCleanupService) columnExists(db *sql.DB, databaseName, tableName, columnName string) (bool, error) {
	// Use INFORMATION_SCHEMA to check column existence with parameterized query
	query := `
		SELECT COUNT(*) 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_NAME = ? 
		AND COLUMN_NAME = ?
	`

	var count int
	err := db.QueryRow(query, databaseName, tableName, columnName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check column %s in table %s: %w", columnName, tableName, err)
	}

	return count > 0, nil
}

// getPrimaryKeyColumns returns the primary key column(s) for a table
func getPrimaryKeyColumns(db *sql.DB, databaseName, tableName string, logger Logger) ([]string, error) {
	query := `
		SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ?
		  AND TABLE_NAME = ?
		  AND COLUMN_KEY = 'PRI'
		ORDER BY ORDINAL_POSITION
	`
	rows, err := db.Query(query, databaseName, tableName)
	if err != nil {
		// Check if it's a connection timeout error
		if strings.Contains(err.Error(), "i/o timeout") || strings.Contains(err.Error(), "connection timed out") {
			return nil, fmt.Errorf("database connection timeout")
		}
		return nil, fmt.Errorf("database query failed: %w", err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to read table structure: %w", err)
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("table %s.%s has no primary key defined", databaseName, tableName)
	}
	logger.Info(fmt.Sprintf("Discovered primary key columns - table: %s, pk_columns: %v", tableName, cols))
	return cols, nil
}

// getTableRowCount returns the total number of rows in a table
func getTableRowCount(db *sql.DB, databaseName, tableName string, whereClause string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s` %s", databaseName, tableName, whereClause)
	var count int
	err := db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %w", tableName, err)
	}
	return count, nil
}

// GofakeitGenerator implements FakeDataGenerator
type GofakeitGenerator struct {
	faker        *gofakeit.Faker
	emailCounter uint64 // Atomic counter for unique email generation
}

func NewGofakeitGenerator() *GofakeitGenerator {
	// Use a combination of time and random for more robust seeding
	seed := time.Now().UnixNano() + rand.Int63()
	return &GofakeitGenerator{
		faker: gofakeit.New(seed),
	}
}

// SchemaAwareGofakeitGenerator implements SchemaAwareFakeDataGenerator
type SchemaAwareGofakeitGenerator struct {
	logger           Logger
	faker            *gofakeit.Faker
	emailCounter     uint64                 // Atomic counter for unique email generation
	staticValueCache map[string]interface{} // Cache for parsed static values
}

func NewSchemaAwareGofakeitGenerator(logger Logger) *SchemaAwareGofakeitGenerator {
	// Use a combination of time and random for more robust seeding
	seed := time.Now().UnixNano() + rand.Int63()
	return &SchemaAwareGofakeitGenerator{
		logger:           logger,
		faker:            gofakeit.New(seed),
		staticValueCache: make(map[string]interface{}),
	}
}

func (g *SchemaAwareGofakeitGenerator) GetColumnInfo(databaseName, tableName, columnName string, db *sql.DB) (*ColumnInfo, error) {
	query := `
		SELECT 
			DATA_TYPE,
			CHARACTER_MAXIMUM_LENGTH,
			IS_NULLABLE,
			COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_NAME = ? 
		AND COLUMN_NAME = ?
	`

	var dataType, isNullable string
	var maxLength sql.NullInt64
	var defaultValue sql.NullString

	err := db.QueryRow(query, databaseName, tableName, columnName).Scan(&dataType, &maxLength, &isNullable, &defaultValue)
	if err != nil {
		return nil, fmt.Errorf("failed to get column info for %s.%s: %w", tableName, columnName, err)
	}

	columnInfo := &ColumnInfo{
		DataType:   dataType,
		IsNullable: isNullable == "YES",
	}

	if maxLength.Valid {
		maxLen := int(maxLength.Int64)
		columnInfo.MaxLength = &maxLen
	}

	if defaultValue.Valid {
		columnInfo.DefaultValue = &defaultValue.String
	}

	maxLenStr := "nil"
	if columnInfo.MaxLength != nil {
		maxLenStr = fmt.Sprintf("%d", *columnInfo.MaxLength)
	}
	g.logger.Debug(fmt.Sprintf("Retrieved column info - table: %s, column: %s, data_type: %s, max_length: %s, is_nullable: %t", tableName, columnName, dataType, maxLenStr, columnInfo.IsNullable))

	return columnInfo, nil
}

func (g *SchemaAwareGofakeitGenerator) GenerateFakeValue(fakerType string, info *ColumnInfo) (interface{}, error) {
	// Generate a value with the basic generator
	value, err := g.generateBasicFakeValue(fakerType)
	if err != nil {
		return nil, err
	}

	// Truncate the value if it's a string and has a max length constraint
	if strValue, ok := value.(string); ok && info.MaxLength != nil {
		if len(strValue) > *info.MaxLength {
			g.logger.Debug(fmt.Sprintf("Truncating string value - original_length: %d, max_length: %d", len(strValue), *info.MaxLength))
			value = strValue[:*info.MaxLength]
		}
	}

	return value, nil
}

func (g *SchemaAwareGofakeitGenerator) generateBasicFakeValue(fakerType string) (interface{}, error) {

	// Handle static_value format (e.g., "static_value: 1", "static_value: true", "static_value: hello")
	if strings.HasPrefix(fakerType, "static_value:") {
		// Check cache first
		if cached, ok := g.staticValueCache[fakerType]; ok {
			return cached, nil
		}

		staticValue := strings.TrimSpace(strings.TrimPrefix(fakerType, "static_value:"))

		var parsedValue interface{}

		// Try to parse as different types
		// First, try as integer
		if num, err := strconv.Atoi(staticValue); err == nil {
			parsedValue = num
		} else if num, err := strconv.ParseFloat(staticValue, 64); err == nil {
			// Try as float
			parsedValue = num
		} else if strings.ToLower(staticValue) == "true" || strings.ToLower(staticValue) == "false" {
			// Try as boolean (case-insensitive)
			parsedValue = strings.ToLower(staticValue) == "true"
		} else if staticValue == "NULL" || staticValue == "null" {
			// Try as NULL
			parsedValue = nil
		} else {
			// Return as string (default)
			parsedValue = staticValue
		}

		// Cache and return
		g.staticValueCache[fakerType] = parsedValue
		return parsedValue, nil
	}

	switch fakerType {
	case "random_email":
		// Generate a more unique email with multiple random components
		baseEmail := g.faker.Email()
		parts := strings.Split(baseEmail, "@")
		if len(parts) == 2 {
			// Add multiple sources of randomness for uniqueness
			suffix1 := g.faker.Number(10, 99)                      // 2-digit random number
			suffix2 := g.faker.Number(100, 999)                    // 3-digit random number
			counter := atomic.AddUint64(&g.emailCounter, 1) % 1000 // Atomic counter instead of time.Now()
			return fmt.Sprintf("%s%d%d%d@%s", parts[0], suffix1, suffix2, counter, parts[1]), nil
		}
		return baseEmail, nil
	case "random_name":
		return g.faker.Name(), nil
	case "random_firstname":
		return g.faker.FirstName(), nil
	case "random_lastname":
		return g.faker.LastName(), nil
	case "random_company":
		return g.faker.Company(), nil
	case "random_address":
		return g.faker.Street(), nil
	case "random_city":
		return g.faker.City(), nil
	case "random_state":
		return g.faker.StateAbr(), nil
	case "random_country_code":
		return g.faker.CountryAbr(), nil
	case "random_postalcode":
		return g.faker.Zip(), nil
	case "random_phone_short":
		// Generate a shorter phone number format (10-15 digits)
		areaCode := g.faker.Number(100, 999)
		prefix := g.faker.Number(100, 999)
		line := g.faker.Number(1000, 9999)
		return fmt.Sprintf("%d-%d-%d", areaCode, prefix, line), nil
	case "random_username":
		return g.faker.Username(), nil
	case "random_id":
		return g.faker.UUID(), nil
	case "random_text":
		// Generate shorter text (1-3 sentences instead of 10)
		return g.faker.Sentence(3), nil
	case "random_word":
		return g.faker.Word(), nil
	case "random_email_subject":
		return g.faker.Sentence(6), nil
	case "random_file_name":
		return g.faker.Word() + g.faker.FileExtension(), nil
	case "random_number_txt":
		return fmt.Sprintf("%d", g.faker.Number(1, 9999)), nil
	case "random_room_number_txt":
		return fmt.Sprintf("%d", g.faker.Number(1, 255)), nil
	default:
		if fakerType == "" {
			return nil, nil // Empty string means don't update this column
		}
		// Check if it's a numeric value (for boolean fields)
		if num, err := strconv.Atoi(fakerType); err == nil {
			return num, nil
		}
		return nil, fmt.Errorf("unknown faker type: %s", fakerType)
	}
}

func (g *GofakeitGenerator) GenerateFakeValue(fakerType string) (interface{}, error) {

	// Handle static_value format (e.g., "static_value: 1", "static_value: true", "static_value: hello")
	if strings.HasPrefix(fakerType, "static_value:") {
		staticValue := strings.TrimSpace(strings.TrimPrefix(fakerType, "static_value:"))

		// Try to parse as different types
		// First, try as integer
		if num, err := strconv.Atoi(staticValue); err == nil {
			return num, nil
		}

		// Try as float
		if num, err := strconv.ParseFloat(staticValue, 64); err == nil {
			return num, nil
		}

		// Try as boolean (case-insensitive)
		if strings.ToLower(staticValue) == "true" || strings.ToLower(staticValue) == "false" {
			return strings.ToLower(staticValue) == "true", nil
		}

		// Try as NULL
		if staticValue == "NULL" || staticValue == "null" {
			return nil, nil
		}

		// Return as string (default)
		return staticValue, nil
	}

	switch fakerType {
	case "random_email":
		// Generate a more unique email with multiple random components
		baseEmail := g.faker.Email()
		parts := strings.Split(baseEmail, "@")
		if len(parts) == 2 {
			// Add multiple sources of randomness for uniqueness
			suffix1 := g.faker.Number(10, 99)                      // 2-digit random number
			suffix2 := g.faker.Number(100, 999)                    // 3-digit random number
			counter := atomic.AddUint64(&g.emailCounter, 1) % 1000 // Atomic counter instead of time.Now()
			return fmt.Sprintf("%s%d%d%d@%s", parts[0], suffix1, suffix2, counter, parts[1]), nil
		}
		return baseEmail, nil
	case "random_name":
		return g.faker.Name(), nil
	case "random_firstname":
		return g.faker.FirstName(), nil
	case "random_lastname":
		return g.faker.LastName(), nil
	case "random_company":
		return g.faker.Company(), nil
	case "random_address":
		return g.faker.Street(), nil
	case "random_city":
		return g.faker.City(), nil
	case "random_state":
		return g.faker.StateAbr(), nil
	case "random_country_code":
		return g.faker.CountryAbr(), nil
	case "random_postalcode":
		return g.faker.Zip(), nil
	case "random_phone_short":
		return g.faker.Phone(), nil
	case "random_username":
		return g.faker.Username(), nil
	case "random_id":
		return g.faker.UUID(), nil
	case "random_text":
		return g.faker.Sentence(10), nil
	case "random_word":
		return g.faker.Word(), nil
	case "random_email_subject":
		return g.faker.Sentence(6), nil
	case "random_file_name":
		return g.faker.Word() + g.faker.FileExtension(), nil
	case "random_number_txt":
		return fmt.Sprintf("%d", g.faker.Number(1000, 9999)), nil
	case "random_room_number_txt":
		return fmt.Sprintf("%d", g.faker.Number(100, 999)), nil
	default:
		if fakerType == "" {
			return nil, nil // Empty string means don't update this column
		}
		// Check if it's a numeric value (for boolean fields)
		if num, err := strconv.Atoi(fakerType); err == nil {
			return num, nil
		}
		return nil, fmt.Errorf("unknown faker type: %s", fakerType)
	}
}

// OSFileReader implements FileReader
type OSFileReader struct{}

func (o *OSFileReader) ReadFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(filename)
}

// ZapLogger implements Logger using zap
type ZapLogger struct {
	logger *zap.Logger
}

func NewZapLogger(debug bool) *ZapLogger {
	// Create a development logger with caller info and stack traces
	config := zap.NewDevelopmentConfig()

	// Set log level based on debug flag
	if debug {
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // Enable debug level
	} else {
		config.Level = zap.NewAtomicLevelAt(zap.InfoLevel) // Only info and above
	}

	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	// Custom caller encoder to show relative paths from repo root
	config.EncoderConfig.EncodeCaller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		// Get the current working directory to make paths relative
		wd, err := os.Getwd()
		if err != nil {
			// Fallback to short caller if we can't get working directory
			enc.AppendString(caller.TrimmedPath())
			return
		}

		// Make the path relative to working directory
		relPath := strings.TrimPrefix(caller.FullPath(), wd+"/")
		if relPath == caller.FullPath() {
			// If trimming didn't work, fall back to just the filename
			relPath = filepath.Base(caller.FullPath())
		}

		// Format as "filename:line"
		enc.AppendString(fmt.Sprintf("%s:%d", relPath, caller.Line))
	}

	logger, err := config.Build()
	if err != nil {
		// Fallback to standard logger if zap fails
		// Note: We can't use structured logging here as zap failed to initialize
		log.Printf("Failed to create zap logger: %v, falling back to std logger", err)
		return &ZapLogger{logger: zap.NewNop()}
	}

	return &ZapLogger{logger: logger}
}

func (z *ZapLogger) Printf(format string, args ...interface{}) {
	z.logger.Sugar().Infof(format, args...)
}

func (z *ZapLogger) Println(args ...interface{}) {
	z.logger.Sugar().Infoln(args...)
}

func (z *ZapLogger) Debug(msg string, fields ...Field) {
	zapFields := z.convertFields(fields)
	z.logger.Debug(msg, zapFields...)
}

func (z *ZapLogger) Info(msg string, fields ...Field) {
	zapFields := z.convertFields(fields)
	z.logger.Info(msg, zapFields...)
}

func (z *ZapLogger) Warn(msg string, fields ...Field) {
	zapFields := z.convertFields(fields)
	z.logger.Warn(msg, zapFields...)
}

func (z *ZapLogger) Error(msg string, fields ...Field) {
	zapFields := z.convertFields(fields)
	z.logger.Error(msg, zapFields...)
}

func (z *ZapLogger) With(fields ...Field) Logger {
	zapFields := z.convertFields(fields)
	return &ZapLogger{logger: z.logger.With(zapFields...)}
}

func (z *ZapLogger) convertFields(fields []Field) []zap.Field {
	zapFields := make([]zap.Field, len(fields))
	for i, field := range fields {
		zapFields[i] = zap.Any(field.Key, field.Value)
	}
	return zapFields
}

// MultiLogger implements Logger that writes to both console and file
type MultiLogger struct {
	consoleLogger Logger
	fileLogger    Logger
}

func NewMultiLogger(consoleLogger Logger, fileLogger Logger) *MultiLogger {
	return &MultiLogger{
		consoleLogger: consoleLogger,
		fileLogger:    fileLogger,
	}
}

func (m *MultiLogger) Printf(format string, args ...interface{}) {
	m.consoleLogger.Printf(format, args...)
	m.fileLogger.Printf(format, args...)
}

func (m *MultiLogger) Println(args ...interface{}) {
	m.consoleLogger.Println(args...)
	m.fileLogger.Println(args...)
}

func (m *MultiLogger) Debug(msg string, fields ...Field) {
	m.consoleLogger.Debug(msg, fields...)
	m.fileLogger.Debug(msg, fields...)
}

func (m *MultiLogger) Info(msg string, fields ...Field) {
	m.consoleLogger.Info(msg, fields...)
	m.fileLogger.Info(msg, fields...)
}

func (m *MultiLogger) Warn(msg string, fields ...Field) {
	m.consoleLogger.Warn(msg, fields...)
	m.fileLogger.Warn(msg, fields...)
}

func (m *MultiLogger) Error(msg string, fields ...Field) {
	m.consoleLogger.Error(msg, fields...)
	m.fileLogger.Error(msg, fields...)
}

func (m *MultiLogger) With(fields ...Field) Logger {
	return &MultiLogger{
		consoleLogger: m.consoleLogger.With(fields...),
		fileLogger:    m.fileLogger.With(fields...),
	}
}

// FileLogger implements Logger that writes to a file
type FileLogger struct {
	file *os.File
}

func NewFileLogger(filename string) (*FileLogger, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	return &FileLogger{file: file}, nil
}

func (f *FileLogger) Printf(format string, args ...interface{}) {
	fmt.Fprintf(f.file, format, args...)
}

func (f *FileLogger) Println(args ...interface{}) {
	fmt.Fprintln(f.file, args...)
}

func (f *FileLogger) Debug(msg string, fields ...Field) {
	fmt.Fprintf(f.file, "[DEBUG] %s\n", msg)
}

func (f *FileLogger) Info(msg string, fields ...Field) {
	fmt.Fprintf(f.file, "[INFO] %s\n", msg)
}

func (f *FileLogger) Warn(msg string, fields ...Field) {
	fmt.Fprintf(f.file, "[WARN] %s\n", msg)
}

func (f *FileLogger) Error(msg string, fields ...Field) {
	fmt.Fprintf(f.file, "[ERROR] %s\n", msg)
}

func (f *FileLogger) With(fields ...Field) Logger {
	return f // FileLogger doesn't support structured logging
}

// StdLogger implements Logger (kept for backward compatibility)
type StdLogger struct{}

func (s *StdLogger) Printf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (s *StdLogger) Println(args ...interface{}) {
	log.Println(args...)
}

func (s *StdLogger) Debug(msg string, fields ...Field) {
	log.Printf("[DEBUG] %s", msg)
}

func (s *StdLogger) Info(msg string, fields ...Field) {
	log.Printf("[INFO] %s", msg)
}

func (s *StdLogger) Warn(msg string, fields ...Field) {
	log.Printf("[WARN] %s", msg)
}

func (s *StdLogger) Error(msg string, fields ...Field) {
	log.Printf("[ERROR] %s", msg)
}

func (s *StdLogger) With(fields ...Field) Logger {
	return s // StdLogger doesn't support structured logging
}

// DebugLogger wraps a logger and only logs debug messages when debug mode is enabled
type DebugLogger struct {
	logger Logger
	debug  bool
}

func (d *DebugLogger) Printf(format string, args ...interface{}) {
	d.logger.Printf(format, args...)
}

func (d *DebugLogger) Println(args ...interface{}) {
	d.logger.Println(args...)
}

func (d *DebugLogger) Debug(msg string, fields ...Field) {
	d.logger.Debug(msg, fields...)
}

func (d *DebugLogger) Info(msg string, fields ...Field) {
	d.logger.Info(msg, fields...)
}

func (d *DebugLogger) Warn(msg string, fields ...Field) {
	d.logger.Warn(msg, fields...)
}

func (d *DebugLogger) Error(msg string, fields ...Field) {
	d.logger.Error(msg, fields...)
}

func (d *DebugLogger) With(fields ...Field) Logger {
	return &DebugLogger{logger: d.logger.With(fields...), debug: d.debug}
}

// MySQLTableFetcher implements TableDataFetcher
type MySQLTableFetcher struct {
	dbConnector DatabaseConnector
	logger      Logger
}

func NewMySQLTableFetcher(dbConnector DatabaseConnector, logger Logger) *MySQLTableFetcher {
	return &MySQLTableFetcher{
		dbConnector: dbConnector,
		logger:      logger,
	}
}

func (m *MySQLTableFetcher) FetchAndDisplayTableData(config Config) error {
	// Connect to database
	db, err := m.dbConnector.Connect(config)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer m.dbConnector.Close(db)

	// Test connection
	if err := m.dbConnector.Ping(db); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	m.logger.Debug("Connected to database", String("database", config.DB))

	// Fetch data from the first table in the list (for backward compatibility)
	if len(config.Tables) == 0 {
		return fmt.Errorf("no tables specified")
	}
	tableName := config.Tables[0]
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 10", tableName)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s: %w", tableName, err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Print column headers
	m.logger.Debug(fmt.Sprintf("Table data - table: %s, columns: %v", tableName, columns))

	// Prepare slice to hold row data
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// Print each row
	rowCount := 0
	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		rowCount++

		// Format row data to show actual values
		rowData := make(map[string]interface{})
		for i, col := range columns {
			// Convert byte slices to strings for better readability
			if b, ok := values[i].([]byte); ok {
				rowData[col] = string(b)
			} else {
				rowData[col] = values[i]
			}
		}

		// Log each field directly for cleaner output
		fields := make([]Field, 0, len(rowData))
		for key, value := range rowData {
			fields = append(fields, Any(key, value))
		}
		m.logger.Debug("Row data", fields...)
	}

	if rowCount == 0 {
		m.logger.Debug(fmt.Sprintf("No data found in table - table: %s", tableName))
	} else {
		m.logger.Debug(fmt.Sprintf("Total rows displayed - table: %s, row_count: %d", tableName, rowCount))
	}

	return nil
}

// processTableWithRange processes a table with optional range filtering
func (d *DataCleanupService) processTableWithRange(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig, pkCols []string, validColumns map[string]string, idRange *IDRange) (int, error) {
	// Build WHERE clause with range and exclude conditions
	var whereConditions []string

	// Add range filtering if specified
	if idRange.HasRange {
		pkCol := pkCols[0] // Already validated to be single-column PK
		rangeClause := idRange.BuildRangeWhereClause(pkCol)
		if rangeClause != "" {
			whereConditions = append(whereConditions, rangeClause)
			d.logger.Debug(fmt.Sprintf("Using range filter - table: %s, range_clause: %s", tableName, rangeClause))
		}
	}

	// Add exclude clause if specified
	if tableConfig.ExcludeClause != "" {
		whereConditions = append(whereConditions, fmt.Sprintf("NOT (%s)", tableConfig.ExcludeClause))
		d.logger.Debug(fmt.Sprintf("Using exclude clause - exclude_clause: %s", tableConfig.ExcludeClause))
	}

	// Build final WHERE clause
	whereClause := ""
	if len(whereConditions) > 0 {
		whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
	}

	// Build SELECT for PK columns
	selectCols := ""
	for i, col := range pkCols {
		if i > 0 {
			selectCols += ", "
		}
		selectCols += fmt.Sprintf("`%s`", col)
	}
	selectQuery := fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s", selectCols, databaseName, tableName, whereClause)
	d.logger.Info(fmt.Sprintf("Executing select query - query: %s", selectQuery))
	startTime := time.Now()
	rows, err := db.Query(selectQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get rows to update: %w", err)
	}
	defer rows.Close()

	// Collect PK values for each row - this is where the actual data fetching happens
	var pkValues [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(pkCols))
		ptrs := make([]interface{}, len(pkCols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			d.logger.Warn(fmt.Sprintf("Failed to scan PK values - table: %s, error: %s", tableName, err))
			continue
		}
		pkValues = append(pkValues, vals)
	}

	// Check for any errors that occurred during iteration
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error during result iteration: %w", err)
	}

	queryDuration := time.Since(startTime)
	d.logger.Info(fmt.Sprintf("Select query and result processing completed - table: %s, duration: %s, rows_collected: %d", tableName, FormatDuration(queryDuration), len(pkValues)))
	if len(pkValues) == 0 {
		d.logger.Info(fmt.Sprintf("Table processing completed - table: %s, no rows to update", tableName))
		return 0, nil
	}

	// Calculate batch count
	batchCount := (len(pkValues) + d.batchProcessor.batchSize - 1) / d.batchProcessor.batchSize

	// Log start of processing at info level
	d.logger.Info(fmt.Sprintf("Starting table processing - table: %s, total_rows: %d, workers: %d, batch_size: %d, batches: %d",
		tableName, len(pkValues), d.batchProcessor.workerCount, d.batchProcessor.batchSize, batchCount))

	// Process rows in parallel batches
	totalProcessed, totalErrors, batchErr := d.processBatchesInParallelPK(db, databaseName, tableName, pkCols, pkValues, tableConfig, validColumns)

	if batchErr != nil {
		return 0, batchErr
	}

	if totalErrors > 0 {
		d.logger.Warn(fmt.Sprintf("Table processing had errors - table: %s, processed: %d, errors: %d",
			tableName, totalProcessed, totalErrors))
	}

	// Log completion at info level - this is the REAL completion after batch processing
	d.logger.Info(fmt.Sprintf("Table processing completed - table: %s, processed: %d, errors: %d",
		tableName, totalProcessed, totalErrors))

	return totalProcessed, nil
}
