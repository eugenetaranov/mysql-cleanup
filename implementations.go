package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"math/rand"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
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
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.DB)
	// Note: We can't log the full DSN as it contains the password
	log.Printf("Connecting to MySQL database: %s@%s:%s/%s", config.User, config.Host, config.Port, config.DB)
	log.Printf("[DEBUG] DSN: %s:****@tcp(%s:%s)/%s?parseTime=true (config.DB=%q)", config.User, config.Host, config.Port, config.DB, config.DB)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
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
			} else if config.Table != "" {
				// Show only the target table
				if tableConfig, exists := dbConfig.Update[config.Table]; exists {
					y.logger.Debug(fmt.Sprintf("Update tables configuration - database: %s, target_table: %s", dbName, config.Table))
					y.logger.Debug(fmt.Sprintf("Table configuration - database: %s, table: %s, column_count: %d", dbName, config.Table, len(tableConfig.Columns)))
					if tableConfig.ExcludeClause != "" {
						y.logger.Debug(fmt.Sprintf("Table exclude clause - database: %s, table: %s, exclude_clause: %s", dbName, config.Table, tableConfig.ExcludeClause))
					}
				} else {
					y.logger.Debug(fmt.Sprintf("Target table not found in config - database: %s, table: %s", dbName, config.Table))
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
	DatabaseName string
	TableName    string
	PKCols       []string
	PKValues     [][]interface{} // Each row's PK values
	TableConfig  TableUpdateConfig
	ValidColumns map[string]string // Pre-validated columns: column -> fakerType
	BatchNumber  int               // Batch number (1, 2, 3, etc.)
	WorkerID     int               // Worker ID that will process this batch
	RowRange     string            // Row range like "1-3" or "4-6"
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
	}
}

func (d *DataCleanupService) CleanupData(config Config) (*CleanupStats, error) {
	startTime := time.Now()
	stats := &CleanupStats{}

	d.logger.Debug(fmt.Sprintf("Starting data cleanup - database: %s, table: %s, all_tables: %t", config.DB, config.Table, config.AllTables))

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
				tableStats, err := d.UpdateTables(db, config.DB, dbConfig.Update)
				if err != nil {
					d.logger.Error(fmt.Sprintf("Failed to update tables - error: %s", err))
					return nil, err
				}
				stats.TotalRowsProcessed += tableStats.TotalRowsProcessed
				stats.TablesProcessed += tableStats.TablesProcessed
			} else {
				// Process only the specified table - go directly to it
				tableConfig, exists := dbConfig.Update[config.Table]
				if !exists {
					d.logger.Debug(fmt.Sprintf("Table not found in config - table: %s, available_tables: %v", config.Table, getKeys(dbConfig.Update)))
					return nil, fmt.Errorf("table '%s' not found in configuration", config.Table)
				}

				d.logger.Debug(fmt.Sprintf("Processing single table - table: %s", config.Table))
				rowsProcessed, err := d.UpdateTableData(db, config.DB, config.Table, tableConfig)
				if err != nil {
					d.logger.Error(fmt.Sprintf("Failed to update table - table: %s, error: %s", config.Table, err))
					return nil, err
				}
				stats.TotalRowsProcessed += rowsProcessed
				stats.TablesProcessed = 1
			}
		}
	}

	stats.TotalDuration = time.Since(startTime)
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

func (d *DataCleanupService) UpdateTables(db *sql.DB, databaseName string, tableConfigs map[string]TableUpdateConfig) (*CleanupStats, error) {
	stats := &CleanupStats{}

	for tableName, tableConfig := range tableConfigs {
		d.logger.Debug(fmt.Sprintf("Updating table - table: %s", tableName))

		rowsProcessed, err := d.UpdateTableData(db, databaseName, tableName, tableConfig)
		if err != nil {
			d.logger.Error(fmt.Sprintf("Failed to update table - table: %s, error: %s", tableName, err))
			return nil, fmt.Errorf("failed to update table %s: %w", tableName, err)
		}

		stats.TotalRowsProcessed += rowsProcessed
		stats.TablesProcessed++

		d.logger.Debug(fmt.Sprintf("Successfully updated table - table: %s", tableName))
	}
	return stats, nil
}

func (d *DataCleanupService) UpdateTableData(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig) (int, error) {
	d.logger.Debug(fmt.Sprintf("Starting parallel table data update - table: %s, worker_count: %d, batch_size: %d",
		tableName, d.batchProcessor.workerCount, d.batchProcessor.batchSize))

	startTime := time.Now()

	// Discover primary key columns
	pkCols, err := getPrimaryKeyColumns(db, databaseName, tableName, d.logger)
	if err != nil {
		return 0, fmt.Errorf("failed to discover primary key: %w", err)
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

	whereClause := ""
	if tableConfig.ExcludeClause != "" {
		whereClause = fmt.Sprintf("WHERE NOT (%s)", tableConfig.ExcludeClause)
		d.logger.Debug(fmt.Sprintf("Using exclude clause - exclude_clause: %s", tableConfig.ExcludeClause))
	}

	// Build SELECT for PK columns
	selectCols := ""
	for i, col := range pkCols {
		if i > 0 {
			selectCols += ", "
		}
		selectCols += fmt.Sprintf("`%s`", col)
	}
	selectQuery := fmt.Sprintf("SELECT %s FROM `%s` %s", selectCols, tableName, whereClause)
	d.logger.Debug(fmt.Sprintf("Executing select query - query: %s", selectQuery))
	rows, err := db.Query(selectQuery)
	if err != nil {
		return 0, fmt.Errorf("failed to get rows to update: %w", err)
	}
	defer rows.Close()

	// Collect PK values for each row
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
	d.logger.Debug(fmt.Sprintf("Scanned PK values - table: %s, row_count: %d", tableName, len(pkValues)))
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
	totalProcessed, totalErrors := d.processBatchesInParallelPK(db, databaseName, tableName, pkCols, pkValues, tableConfig, validColumns)
	duration := time.Since(startTime)

	// Log completion at info level with formatted duration
	d.logger.Info(fmt.Sprintf("Table processing completed - table: %s, total_rows: %d, processed: %d, errors: %d, duration: %s, rows_per_second: %.1f",
		tableName, len(pkValues), totalProcessed, totalErrors, FormatDuration(duration), float64(totalProcessed)/duration.Seconds()))

	d.logger.Debug(fmt.Sprintf("Parallel batch processing completed - table: %s, total_rows: %d, processed: %d, errors: %d, duration: %s, rows_per_second: %.1f",
		tableName, len(pkValues), totalProcessed, totalErrors, FormatDuration(duration), float64(totalProcessed)/duration.Seconds()))

	return totalProcessed, nil
}

// processBatchesInParallelPK processes row batches using a worker pool (PK-aware)
func (d *DataCleanupService) processBatchesInParallelPK(db *sql.DB, databaseName, tableName string, pkCols []string, pkValues [][]interface{}, tableConfig TableUpdateConfig, validColumns map[string]string) (int, int) {
	batches := d.createBatchesPK(pkValues, d.batchProcessor.batchSize)

	jobChan := make(chan BatchJob, len(batches))
	resultChan := make(chan BatchResult, len(batches))

	var wg sync.WaitGroup
	for i := 0; i < d.batchProcessor.workerCount; i++ {
		wg.Add(1)
		// Each worker gets its own schema-aware generator with a truly unique seed
		// Combine multiple factors to ensure unique seeds: time + worker ID + random component
		uniqueSeed := time.Now().UnixNano() + int64(i*1000000) + rand.Int63()
		workerGenerator := NewSchemaAwareGofakeitGenerator(d.logger)
		workerGenerator.faker = gofakeit.New(uniqueSeed)
		go d.workerPK(i, db, workerGenerator, jobChan, resultChan, &wg)

		// Small delay to ensure different timestamps for each worker
		time.Sleep(time.Microsecond)
	}

	go func() {
		defer close(jobChan)
		for batchIndex, batch := range batches {
			// Calculate row range from the first and last PK values in the batch
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

			jobChan <- BatchJob{
				DatabaseName: databaseName,
				TableName:    tableName,
				PKCols:       pkCols,
				PKValues:     batch,
				TableConfig:  tableConfig,
				ValidColumns: validColumns,
				BatchNumber:  batchIndex + 1, // 1-based batch numbering
				WorkerID:     -1,             // Will be set by worker
				RowRange:     rowRange,
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	totalProcessed := 0
	totalErrors := 0
	for result := range resultChan {
		totalProcessed += result.ProcessedCount
		totalErrors += result.ErrorCount

		if result.ErrorCount > 0 {
			d.logger.Warn(fmt.Sprintf("Worker %d batch %d had errors - table: %s, row_range: %s, processed: %d, errors: %d, duration: %s",
				result.WorkerID, result.BatchNumber, result.TableName, result.RowRange, result.ProcessedCount, result.ErrorCount, FormatDuration(result.Duration)))
		} else {
			d.logger.Debug(fmt.Sprintf("Worker %d batch %d completed successfully - table: %s, row_range: %s, processed: %d, duration: %s",
				result.WorkerID, result.BatchNumber, result.TableName, result.RowRange, result.ProcessedCount, FormatDuration(result.Duration)))
		}
	}
	return totalProcessed, totalErrors
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

// workerPK processes PK-aware batch jobs
func (d *DataCleanupService) workerPK(workerID int, db *sql.DB, generator *SchemaAwareGofakeitGenerator, jobChan <-chan BatchJob, resultChan chan<- BatchResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		// Set the worker ID on the job
		job.WorkerID = workerID

		startTime := time.Now()
		processed, errors := d.processBatchPK(db, generator, job)
		duration := time.Since(startTime)
		resultChan <- BatchResult{
			BatchJob:       job,
			ProcessedCount: processed,
			ErrorCount:     errors,
			Duration:       duration,
		}
	}
}

// processBatchPK processes a single batch of rows using bulk UPDATE with PK(s)
func (d *DataCleanupService) processBatchPK(db *sql.DB, generator *SchemaAwareGofakeitGenerator, job BatchJob) (int, int) {
	if len(job.PKValues) == 0 {
		return 0, 0
	}

	d.logger.Debug(fmt.Sprintf("Worker %d batch %d starting - table: %s, row_range: %s, batch_size: %d",
		job.WorkerID, job.BatchNumber, job.TableName, job.RowRange, len(job.PKValues)))

	// Build UPDATE query for each row in batch
	processed := 0
	errors := 0

	for _, pkVals := range job.PKValues {
		// Generate fake values for all pre-validated columns for each row
		columnUpdates := make(map[string]interface{})
		for column, fakerType := range job.ValidColumns {
			// Get column info from cache
			d.cacheMutex.RLock()
			info := d.columnInfoCache[job.TableName][column]
			d.cacheMutex.RUnlock()

			fakeValue, err := generator.GenerateFakeValue(fakerType, info)
			if err != nil {
				d.logger.Warn(fmt.Sprintf("Failed to generate fake value - table: %s, column: %s, faker_type: %s, error: %s", job.TableName, column, fakerType, err))
				continue
			}
			columnUpdates[column] = fakeValue
		}
		if len(columnUpdates) == 0 {
			continue // Skip row if no valid columns to update
		}

		setParts := make([]string, 0, len(columnUpdates))
		args := make([]interface{}, 0, len(columnUpdates)+len(pkVals))
		for col, val := range columnUpdates {
			setParts = append(setParts, fmt.Sprintf("`%s` = ?", col))
			args = append(args, val)
		}
		// WHERE clause for PK(s)
		whereParts := make([]string, len(job.PKCols))
		for i, pkCol := range job.PKCols {
			whereParts[i] = fmt.Sprintf("`%s` = ?", pkCol)
			args = append(args, pkVals[i])
		}
		query := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s", job.DatabaseName, job.TableName, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))
		_, err := db.Exec(query, args...)
		if err != nil {
			d.logger.Error(fmt.Sprintf("Failed to update row - table: %s, error: %s", job.TableName, err))
			errors++
			continue
		}

		// Log the row update in real-time
		pkStr := ""
		for i, pkCol := range job.PKCols {
			if pkStr != "" {
				pkStr += ", "
			}
			pkStr += fmt.Sprintf("%s:%v", pkCol, pkVals[i])
		}
		updateStr := ""
		for col, val := range columnUpdates {
			if updateStr != "" {
				updateStr += ", "
			}
			updateStr += fmt.Sprintf("%s:%v", col, val)
		}
		d.logger.Debug(fmt.Sprintf("Worker %d batch %d updated row - %s -> %s", job.WorkerID, job.BatchNumber, pkStr, updateStr))

		processed++
	}

	return processed, errors
}

// formatSampleData formats sample row data for debug logging
func (d *DataCleanupService) formatSampleData(sampleData []SampleRowData) string {
	if len(sampleData) == 0 {
		return "no sample data"
	}

	var parts []string
	for i, sample := range sampleData {
		if i >= 4 { // Show max 4 sample rows to avoid too much logging
			parts = append(parts, "...")
			break
		}

		pkStr := ""
		for col, val := range sample.PKValues {
			if pkStr != "" {
				pkStr += ", "
			}
			pkStr += fmt.Sprintf("%s:%v", col, val)
		}

		updateStr := ""
		for col, val := range sample.UpdatedColumns {
			if updateStr != "" {
				updateStr += ", "
			}
			// Show complete values to verify uniqueness
			valStr := fmt.Sprintf("%v", val)
			updateStr += fmt.Sprintf("%s:'%s'", col, valStr)
		}

		parts = append(parts, fmt.Sprintf("[PK:{%s} -> {%s}]", pkStr, updateStr))
	}

	return strings.Join(parts, ", ")
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
		logger.Error(fmt.Sprintf("Failed to query primary key columns - error: %s, table: %s", err, tableName))
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			logger.Error(fmt.Sprintf("Failed to scan PK column - error: %s, table: %s", err, tableName))
			return nil, err
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		logger.Error(fmt.Sprintf("No primary key found for table - table: %s", tableName))
		return nil, fmt.Errorf("no primary key found for table %s", tableName)
	}
	logger.Info(fmt.Sprintf("Discovered primary key columns - table: %s, pk_columns: %v", tableName, cols))
	return cols, nil
}

// GofakeitGenerator implements FakeDataGenerator
type GofakeitGenerator struct {
	faker *gofakeit.Faker
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
	logger Logger
	faker  *gofakeit.Faker
}

func NewSchemaAwareGofakeitGenerator(logger Logger) *SchemaAwareGofakeitGenerator {
	// Use a combination of time and random for more robust seeding
	seed := time.Now().UnixNano() + rand.Int63()
	return &SchemaAwareGofakeitGenerator{
		logger: logger,
		faker:  gofakeit.New(seed),
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

	switch fakerType {
	case "random_email":
		// Generate a more unique email with multiple random components
		baseEmail := g.faker.Email()
		parts := strings.Split(baseEmail, "@")
		if len(parts) == 2 {
			// Add multiple sources of randomness for uniqueness
			suffix1 := g.faker.Number(10, 99)             // 2-digit random number
			suffix2 := g.faker.Number(100, 999)           // 3-digit random number
			timeComponent := time.Now().UnixNano() % 1000 // Time-based component
			return fmt.Sprintf("%s%d%d%d@%s", parts[0], suffix1, suffix2, timeComponent, parts[1]), nil
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

	switch fakerType {
	case "random_email":
		// Generate a more unique email with multiple random components
		baseEmail := g.faker.Email()
		parts := strings.Split(baseEmail, "@")
		if len(parts) == 2 {
			// Add multiple sources of randomness for uniqueness
			suffix1 := g.faker.Number(10, 99)             // 2-digit random number
			suffix2 := g.faker.Number(100, 999)           // 3-digit random number
			timeComponent := time.Now().UnixNano() % 1000 // Time-based component
			return fmt.Sprintf("%s%d%d%d@%s", parts[0], suffix1, suffix2, timeComponent, parts[1]), nil
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

	// Fetch data from table
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 10", config.Table)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query table %s: %w", config.Table, err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// Print column headers
	m.logger.Debug(fmt.Sprintf("Table data - table: %s, columns: %v", config.Table, columns))

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
		m.logger.Debug(fmt.Sprintf("No data found in table - table: %s", config.Table))
	} else {
		m.logger.Debug(fmt.Sprintf("Total rows displayed - table: %s, row_count: %d", config.Table, rowCount))
	}

	return nil
}
