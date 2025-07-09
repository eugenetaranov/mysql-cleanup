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

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v2"
)

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
		y.logger.Info("Config path is S3 URI, downloading", String("s3_uri", configPath))
		localPath, err := y.s3Handler.DownloadS3File(configPath)
		if err != nil {
			y.logger.Error("Failed to download config from S3", String("s3_uri", configPath), Error("error", err))
			return nil, err
		}
		tempFilePath = localPath
		configPath = localPath
		cleanup = func() {
			if err := y.s3Handler.CleanupTempFile(tempFilePath); err != nil {
				y.logger.Error("Failed to cleanup temp file", Error("error", err))
			}
		}
		defer cleanup()
		y.logger.Info("Downloaded S3 config to local file", String("local_path", localPath))
	}
	
	y.logger.Debug("Reading config file", String("path", configPath))
	data, err := y.fileReader.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	y.logger.Debug("Parsing YAML data", Int("bytes", len(data)))
	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	y.logger.Debug("YAML config parsed successfully", Int("database_count", len(yamlConfig.Databases)))
	return &yamlConfig, nil
}

func (y *YAMLConfigParser) ParseAndDisplayConfig(configPath string) error {
	yamlConfig, err := y.ParseConfig(configPath)
	if err != nil {
		return err
	}

	y.displayConfig(*yamlConfig)
	return nil
}

func (y *YAMLConfigParser) displayConfig(config YAMLConfig) {
	for dbName, dbConfig := range config.Databases {
		y.logger.Debug("Database configuration", String("database", dbName), String("truncate_tables", fmt.Sprintf("%v", dbConfig.Truncate)))
		
		if len(dbConfig.Update) > 0 {
			y.logger.Debug("Update tables configuration", String("database", dbName), Int("update_table_count", len(dbConfig.Update)))
			for tableName, tableConfig := range dbConfig.Update {
				y.logger.Debug("Table configuration", String("database", dbName), String("table", tableName), Int("column_count", len(tableConfig.Columns)))
				if tableConfig.ExcludeClause != "" {
					y.logger.Debug("Table exclude clause", String("database", dbName), String("table", tableName), String("exclude_clause", tableConfig.ExcludeClause))
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
}

// BatchResult represents the result of processing a batch
type BatchResult struct {
	BatchJob
	ProcessedCount int
	ErrorCount     int
	Duration       time.Duration
	Errors         []error
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
	dbConnector    DatabaseConnector
	configParser   ConfigParser
	fakeGenerator  FakeDataGenerator
	schemaAwareGenerator SchemaAwareFakeDataGenerator
	logger         Logger
	batchProcessor *BatchProcessor
}

func NewDataCleanupService(dbConnector DatabaseConnector, configParser ConfigParser, fakeGenerator FakeDataGenerator, schemaAwareGenerator SchemaAwareFakeDataGenerator, logger Logger, workers, batchSize int) *DataCleanupService {
	return &DataCleanupService{
		dbConnector:    dbConnector,
		configParser:   configParser,
		fakeGenerator:  fakeGenerator,
		schemaAwareGenerator: schemaAwareGenerator,
		logger:         logger,
		batchProcessor: NewBatchProcessor(logger, workers, batchSize),
	}
}

func (d *DataCleanupService) CleanupData(config Config) error {
	d.logger.Debug("Starting data cleanup", String("database", config.DB), String("table", config.Table), String("all_tables", fmt.Sprintf("%t", config.AllTables)))
	
	// Parse the YAML configuration
	yamlConfig, err := d.configParser.ParseConfig(config.Config)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Connect to database
	d.logger.Debug("Connecting to database")
	db, err := d.dbConnector.Connect(config)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer d.dbConnector.Close(db)

	// Process each database in the config
	d.logger.Debug("Processing databases from config", Int("total_databases", len(yamlConfig.Databases)))
	for dbName, dbConfig := range yamlConfig.Databases {
		d.logger.Debug("Checking database", String("config_db", dbName), String("target_db", config.DB))
		if dbName != config.DB {
			d.logger.Debug("Skipping database", String("database", dbName), String("reason", "not target database"))
			continue // Skip if not the target database
		}

		d.logger.Debug("Processing database", String("database", dbName))

		// Handle truncate tables
		d.logger.Debug("Checking truncate tables", Int("truncate_count", len(dbConfig.Truncate)))
		if len(dbConfig.Truncate) > 0 {
			if err := d.TruncateTables(db, dbConfig.Truncate); err != nil {
				d.logger.Warn("Failed to truncate tables", Error("error", err))
			}
		}

		// Handle update tables
		d.logger.Debug("Checking update tables", Int("update_count", len(dbConfig.Update)))
		if len(dbConfig.Update) > 0 {
			if config.AllTables {
				// Process all tables
				d.logger.Debug("Processing all tables mode")
				d.logger.Debug("Processing all tables", Int("table_count", len(dbConfig.Update)))
				if err := d.UpdateTables(db, config.DB, dbConfig.Update); err != nil {
					d.logger.Error("Failed to update tables", Error("error", err))
					return err
				}
			} else {
				// Process only the specified table
				d.logger.Debug("Processing single table mode", String("target_table", config.Table))
				tableConfig, exists := dbConfig.Update[config.Table]
				if !exists {
					d.logger.Debug("Table not found in config", String("table", config.Table), String("available_tables", fmt.Sprintf("%v", getKeys(dbConfig.Update))))
					return fmt.Errorf("table '%s' not found in configuration", config.Table)
				}

				d.logger.Debug("Processing single table", String("table", config.Table))
				if err := d.UpdateTableData(db, config.DB, config.Table, tableConfig); err != nil {
					d.logger.Error("Failed to update table", String("table", config.Table), Error("error", err))
					return err
				}
			}
		}
	}

	return nil
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
		d.logger.Debug("Truncating table", String("table", table))
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		if _, err := db.Exec(query); err != nil {
			d.logger.Error("Failed to truncate table", String("table", table), Error("error", err))
			return fmt.Errorf("failed to truncate table %s: %w", table, err)
		}
		d.logger.Debug("Successfully truncated table", String("table", table))
	}
	return nil
}

func (d *DataCleanupService) UpdateTables(db *sql.DB, databaseName string, tableConfigs map[string]TableUpdateConfig) error {
	for tableName, tableConfig := range tableConfigs {
		d.logger.Debug("Updating table", String("table", tableName))
		
		if err := d.UpdateTableData(db, databaseName, tableName, tableConfig); err != nil {
			d.logger.Error("Failed to update table", String("table", tableName), Error("error", err))
			return fmt.Errorf("failed to update table %s: %w", tableName, err)
		}
		
		d.logger.Debug("Successfully updated table", String("table", tableName))
	}
	return nil
}

func (d *DataCleanupService) UpdateTableData(db *sql.DB, databaseName, tableName string, tableConfig TableUpdateConfig) error {
	d.logger.Debug("Starting parallel table data update", String("table", tableName), Int("column_count", len(tableConfig.Columns)), Int("worker_count", d.batchProcessor.workerCount), Int("batch_size", d.batchProcessor.batchSize))

	startTime := time.Now()

	// Discover primary key columns
	pkCols, err := getPrimaryKeyColumns(db, databaseName, tableName, d.logger)
	if err != nil {
		return fmt.Errorf("failed to discover primary key: %w", err)
	}

	// Pre-check all columns exist before starting parallel processing
	validColumns := make(map[string]string) // column -> fakerType
	for column, fakerType := range tableConfig.Columns {
		exists, err := d.columnExists(db, databaseName, tableName, column)
		if err != nil {
			d.logger.Warn("Failed to check column", String("table", tableName), String("column", column), Error("error", err))
			continue
		}
		if !exists {
			d.logger.Warn("Column does not exist, skipping", String("table", tableName), String("column", column))
			continue
		}
		validColumns[column] = fakerType
	}
	
	if len(validColumns) == 0 {
		d.logger.Warn("No valid columns found for update", String("table", tableName))
		return nil
	}
	
	d.logger.Debug("Pre-validated columns", String("table", tableName), Int("valid_columns", len(validColumns)))

	whereClause := ""
	if tableConfig.ExcludeClause != "" {
		whereClause = fmt.Sprintf("WHERE NOT (%s)", tableConfig.ExcludeClause)
		d.logger.Debug("Using exclude clause", String("exclude_clause", tableConfig.ExcludeClause))
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
	d.logger.Debug("Executing select query", String("query", selectQuery))
	rows, err := db.Query(selectQuery)
	if err != nil {
		return fmt.Errorf("failed to get rows to update: %w", err)
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
			d.logger.Warn("Failed to scan PK values", String("table", tableName), Error("error", err))
			continue
		}
		pkValues = append(pkValues, vals)
	}
	d.logger.Debug("Scanned PK values", String("table", tableName), Int("row_count", len(pkValues)))
	if len(pkValues) == 0 {
		d.logger.Debug("No rows to update", String("table", tableName))
		return nil
	}
	d.logger.Debug("Starting parallel batch processing", String("table", tableName), Int("row_count", len(pkValues)), Int("batch_size", d.batchProcessor.batchSize))

	// Process rows in parallel batches
	totalProcessed, totalErrors := d.processBatchesInParallelPK(db, databaseName, tableName, pkCols, pkValues, tableConfig, validColumns)
	duration := time.Since(startTime)
	d.logger.Debug("Parallel batch processing completed", 
		String("table", tableName), 
		Int("total_rows", len(pkValues)),
		Int("processed", totalProcessed),
		Int("errors", totalErrors),
		String("duration", duration.String()),
		String("rows_per_second", fmt.Sprintf("%.2f", float64(totalProcessed)/duration.Seconds())))

	return nil
}

// processBatchesInParallelPK processes row batches using a worker pool (PK-aware)
func (d *DataCleanupService) processBatchesInParallelPK(db *sql.DB, databaseName, tableName string, pkCols []string, pkValues [][]interface{}, tableConfig TableUpdateConfig, validColumns map[string]string) (int, int) {
	batches := d.createBatchesPK(pkValues, d.batchProcessor.batchSize)
	d.logger.Debug("Created PK batches", String("table", tableName), Int("batch_count", len(batches)), Int("batch_size", d.batchProcessor.batchSize))

	jobChan := make(chan BatchJob, len(batches))
	resultChan := make(chan BatchResult, len(batches))

	var wg sync.WaitGroup
	for i := 0; i < d.batchProcessor.workerCount; i++ {
		wg.Add(1)
		go d.workerPK(i, db, jobChan, resultChan, &wg)
	}

			go func() {
			defer close(jobChan)
			for _, batch := range batches {
				jobChan <- BatchJob{
					DatabaseName: databaseName,
					TableName:    tableName,
					PKCols:       pkCols,
					PKValues:     batch,
					TableConfig:  tableConfig,
					ValidColumns: validColumns,
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
			d.logger.Warn("Batch had errors", String("table", result.TableName), Int("processed", result.ProcessedCount), Int("errors", result.ErrorCount), String("duration", result.Duration.String()))
		} else {
			d.logger.Debug("Batch completed successfully", String("table", result.TableName), Int("processed", result.ProcessedCount), String("duration", result.Duration.String()))
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
func (d *DataCleanupService) workerPK(workerID int, db *sql.DB, jobChan <-chan BatchJob, resultChan chan<- BatchResult, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobChan {
		startTime := time.Now()
		processed, errors := d.processBatchPK(db, job)
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
func (d *DataCleanupService) processBatchPK(db *sql.DB, job BatchJob) (int, int) {
	if len(job.PKValues) == 0 {
		return 0, 0
	}

	// Generate fake values for all pre-validated columns once (reuse for all rows in batch)
	columnUpdates := make(map[string]interface{})
	for column, fakerType := range job.ValidColumns {
		fakeValue, err := d.schemaAwareGenerator.GenerateFakeValue(fakerType, job.DatabaseName, job.TableName, column, db)
		if err != nil {
			d.logger.Warn("Failed to generate fake value", String("table", job.TableName), String("column", column), String("faker_type", fakerType), Error("error", err))
			continue
		}
		columnUpdates[column] = fakeValue
	}
	if len(columnUpdates) == 0 {
		return 0, 0
	}

	// Build UPDATE query for each row in batch
	processed := 0
	errors := 0
	for _, pkVals := range job.PKValues {
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
			d.logger.Error("Failed to update row", String("table", job.TableName), Error("error", err))
			errors++
			continue
		}
		processed++
	}
	return processed, errors
}



func (d *DataCleanupService) updateSingleRow(db *sql.DB, databaseName, tableName string, rowID int64, tableConfig TableUpdateConfig) error {
	d.logger.Debug("Updating single row", String("table", tableName), Int64("row_id", rowID), Int("column_count", len(tableConfig.Columns)))

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
			d.logger.Warn("Failed to check column", String("table", tableName), String("column", column), Error("error", err))
			continue
		}
		if !exists {
			d.logger.Warn("Column does not exist, skipping", String("table", tableName), String("column", column))
			continue
		}

		fakeValue, err := d.schemaAwareGenerator.GenerateFakeValue(fakerType, databaseName, tableName, column, db)
		if err != nil {
			d.logger.Warn("Failed to generate fake value", String("table", tableName), String("column", column), String("faker_type", fakerType), Error("error", err))
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
	d.logger.Debug("Executing update query", String("table", tableName), Int64("row_id", rowID), String("query", query))

	result, err := db.Exec(query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute UPDATE query for row %d: %w", rowID, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		d.logger.Warn("No rows affected", String("table", tableName), Int64("row_id", rowID))
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
		logger.Error("Failed to query primary key columns", Error("error", err), String("table", tableName))
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			logger.Error("Failed to scan PK column", Error("error", err), String("table", tableName))
			return nil, err
		}
		cols = append(cols, col)
	}
	if len(cols) == 0 {
		logger.Error("No primary key found for table", String("table", tableName))
		return nil, fmt.Errorf("no primary key found for table %s", tableName)
	}
	logger.Info("Discovered primary key columns", String("table", tableName), String("pk_columns", fmt.Sprintf("%v", cols)))
	return cols, nil
}

// GofakeitGenerator implements FakeDataGenerator
type GofakeitGenerator struct{}

// SchemaAwareGofakeitGenerator implements SchemaAwareFakeDataGenerator
type SchemaAwareGofakeitGenerator struct {
	logger Logger
}

func NewSchemaAwareGofakeitGenerator(logger Logger) *SchemaAwareGofakeitGenerator {
	return &SchemaAwareGofakeitGenerator{
		logger: logger,
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
	
	g.logger.Debug("Retrieved column info", 
		String("table", tableName), 
		String("column", columnName), 
		String("data_type", dataType),
		Any("max_length", columnInfo.MaxLength),
		Bool("is_nullable", columnInfo.IsNullable))
	
	return columnInfo, nil
}

func (g *SchemaAwareGofakeitGenerator) GenerateFakeValue(fakerType string, databaseName, tableName, columnName string, db *sql.DB) (interface{}, error) {
	g.logger.Debug("Generating schema-aware fake value", 
		String("faker_type", fakerType), 
		String("table", tableName), 
		String("column", columnName))
	
	// Get column information
	columnInfo, err := g.GetColumnInfo(databaseName, tableName, columnName, db)
	if err != nil {
		g.logger.Warn("Failed to get column info, falling back to basic generation", 
			String("table", tableName), 
			String("column", columnName), 
			Error("error", err))
		// Fall back to basic generation without schema awareness
		return g.generateBasicFakeValue(fakerType)
	}
	
	// Generate the basic fake value
	value, err := g.generateBasicFakeValue(fakerType)
	if err != nil {
		return nil, err
	}
	
	// Truncate the value if it's a string and has a max length constraint
	if strValue, ok := value.(string); ok && columnInfo.MaxLength != nil {
		if len(strValue) > *columnInfo.MaxLength {
			g.logger.Debug("Truncating string value", 
				String("table", tableName), 
				String("column", columnName), 
				Int("original_length", len(strValue)), 
				Int("max_length", *columnInfo.MaxLength))
			value = strValue[:*columnInfo.MaxLength]
		}
	}
	
	return value, nil
}

func (g *SchemaAwareGofakeitGenerator) generateBasicFakeValue(fakerType string) (interface{}, error) {
	// Note: We can't use structured logging here as this generator doesn't have a logger
	log.Printf("[DEBUG] Generating fake value for type: %s", fakerType)
	
	switch fakerType {
	case "random_email":
		// Use gofakeit.Email() for realism, but add a short random number to the username for uniqueness
		email := gofakeit.Email()
		parts := strings.Split(email, "@")
		if len(parts) == 2 {
			suffix := gofakeit.Number(10, 99) // 2-digit random number
			return fmt.Sprintf("%s%d@%s", parts[0], suffix, parts[1]), nil
		}
		return email, nil
	case "random_name":
		return gofakeit.Name(), nil
	case "random_firstname":
		return gofakeit.FirstName(), nil
	case "random_lastname":
		return gofakeit.LastName(), nil
	case "random_company":
		return gofakeit.Company(), nil
	case "random_address":
		return gofakeit.Street(), nil
	case "random_city":
		return gofakeit.City(), nil
	case "random_state":
		return gofakeit.StateAbr(), nil
	case "random_country_code":
		return gofakeit.CountryAbr(), nil
	case "random_postalcode":
		return gofakeit.Zip(), nil
	case "random_phone_short":
		return gofakeit.Phone(), nil
	case "random_username":
		return gofakeit.Username(), nil
	case "random_id":
		return gofakeit.UUID(), nil
	case "random_text":
		return gofakeit.Sentence(10), nil
	case "random_word":
		return gofakeit.Word(), nil
	case "random_email_subject":
		return gofakeit.Sentence(6), nil
	case "random_file_name":
		return gofakeit.Word() + gofakeit.FileExtension(), nil
	case "random_number_txt":
		return fmt.Sprintf("%d", gofakeit.Number(1, 9999)), nil
	case "random_room_number_txt":
		return fmt.Sprintf("%d", gofakeit.Number(1, 255)), nil
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
	// Note: We can't use structured logging here as this generator doesn't have a logger
	log.Printf("[DEBUG] Generating fake value for type: %s", fakerType)
	
	switch fakerType {
	case "random_email":
		// Use gofakeit.Email() for realism, but add a short random number to the username for uniqueness
		email := gofakeit.Email()
		parts := strings.Split(email, "@")
		if len(parts) == 2 {
			suffix := gofakeit.Number(10, 99) // 2-digit random number
			return fmt.Sprintf("%s%d@%s", parts[0], suffix, parts[1]), nil
		}
		return email, nil
	case "random_name":
		return gofakeit.Name(), nil
	case "random_firstname":
		return gofakeit.FirstName(), nil
	case "random_lastname":
		return gofakeit.LastName(), nil
	case "random_company":
		return gofakeit.Company(), nil
	case "random_address":
		return gofakeit.Street(), nil
	case "random_city":
		return gofakeit.City(), nil
	case "random_state":
		return gofakeit.StateAbr(), nil
	case "random_country_code":
		return gofakeit.CountryAbr(), nil
	case "random_postalcode":
		return gofakeit.Zip(), nil
	case "random_phone_short":
		return gofakeit.Phone(), nil
	case "random_username":
		return gofakeit.Username(), nil
	case "random_id":
		return gofakeit.UUID(), nil
	case "random_text":
		return gofakeit.Sentence(10), nil
	case "random_word":
		return gofakeit.Word(), nil
	case "random_email_subject":
		return gofakeit.Sentence(6), nil
	case "random_file_name":
		return gofakeit.Word() + gofakeit.FileExtension(), nil
	case "random_number_txt":
		return fmt.Sprintf("%d", gofakeit.Number(1000, 9999)), nil
	case "random_room_number_txt":
		return fmt.Sprintf("%d", gofakeit.Number(100, 999)), nil
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
	m.logger.Debug("Table data", String("table", config.Table), String("columns", fmt.Sprintf("%v", columns)))

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
		m.logger.Debug("Row data", Int("row_number", rowCount), String("table", config.Table))
	}

	if rowCount == 0 {
		m.logger.Debug("No data found in table", String("table", config.Table))
	} else {
		m.logger.Debug("Total rows displayed", String("table", config.Table), Int("row_count", rowCount))
	}

	return nil
} 

 