package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/brianvoe/gofakeit/v7"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

// MySQLConnector implements DatabaseConnector
type MySQLConnector struct{}

func (m *MySQLConnector) Connect(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", 
		config.User, config.Password, config.Host, config.Port, config.DB)
	
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
	logger     Logger
}

func NewYAMLConfigParser(fileReader FileReader, logger Logger) *YAMLConfigParser {
	return &YAMLConfigParser{
		fileReader: fileReader,
		logger:     logger,
	}
}

func (y *YAMLConfigParser) ParseConfig(configPath string) (*YAMLConfig, error) {
	data, err := y.fileReader.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

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
		y.logger.Printf("\nDatabase: %s", dbName)
		y.logger.Printf("  Truncate tables: %v", dbConfig.Truncate)
		
		if len(dbConfig.Update) > 0 {
			y.logger.Printf("  Update tables:")
			for tableName, tableConfig := range dbConfig.Update {
				y.logger.Printf("    %s:", tableName)
				y.logger.Printf("      Columns to update:")
				for column, value := range tableConfig.Columns {
					y.logger.Printf("        %s: %s", column, value)
				}
				if tableConfig.ExcludeClause != "" {
					y.logger.Printf("      Exclude clause: %s", tableConfig.ExcludeClause)
				}
			}
		}
	}
}

// DataCleanupService implements DataCleaner
type DataCleanupService struct {
	dbConnector   DatabaseConnector
	configParser  ConfigParser
	fakeGenerator FakeDataGenerator
	logger        Logger
}

func NewDataCleanupService(dbConnector DatabaseConnector, configParser ConfigParser, fakeGenerator FakeDataGenerator, logger Logger) *DataCleanupService {
	return &DataCleanupService{
		dbConnector:   dbConnector,
		configParser:  configParser,
		fakeGenerator: fakeGenerator,
		logger:        logger,
	}
}

func (d *DataCleanupService) CleanupData(config Config) error {
	// Parse the YAML configuration
	yamlConfig, err := d.configParser.ParseConfig(config.Config)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Connect to database
	db, err := d.dbConnector.Connect(config)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer d.dbConnector.Close(db)

	// Process each database in the config
	for dbName, dbConfig := range yamlConfig.Databases {
		if dbName != config.DB {
			continue // Skip if not the target database
		}

		d.logger.Info("Processing database", String("database", dbName))

		// Handle truncate tables
		if len(dbConfig.Truncate) > 0 {
			if err := d.TruncateTables(db, dbConfig.Truncate); err != nil {
				d.logger.Printf("Warning: failed to truncate tables: %v", err)
			}
		}

		// Handle update tables
		if len(dbConfig.Update) > 0 {
			if err := d.UpdateTables(db, dbConfig.Update); err != nil {
				d.logger.Printf("Warning: failed to update tables: %v", err)
			}
		}
	}

	return nil
}

func (d *DataCleanupService) TruncateTables(db *sql.DB, tables []string) error {
	for _, table := range tables {
		d.logger.Info("Truncating table", String("table", table))
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		if _, err := db.Exec(query); err != nil {
			d.logger.Error("Failed to truncate table", String("table", table), Error("error", err))
			return fmt.Errorf("failed to truncate table %s: %w", table, err)
		}
		d.logger.Info("Successfully truncated table", String("table", table))
	}
	return nil
}

func (d *DataCleanupService) UpdateTables(db *sql.DB, tableConfigs map[string]TableUpdateConfig) error {
	for tableName, tableConfig := range tableConfigs {
		d.logger.Info("Updating table", String("table", tableName))
		
		if err := d.UpdateTableData(db, tableName, tableConfig); err != nil {
			d.logger.Error("Failed to update table", String("table", tableName), Error("error", err))
			return fmt.Errorf("failed to update table %s: %w", tableName, err)
		}
		
		d.logger.Info("Successfully updated table", String("table", tableName))
	}
	return nil
}

func (d *DataCleanupService) UpdateTableData(db *sql.DB, tableName string, tableConfig TableUpdateConfig) error {
	// First, get all row IDs that need to be updated
	whereClause := ""
	if tableConfig.ExcludeClause != "" {
		whereClause = fmt.Sprintf("WHERE NOT (%s)", tableConfig.ExcludeClause)
	}

	// Get all row IDs to update
	selectQuery := fmt.Sprintf("SELECT id FROM `%s` %s", tableName, whereClause)
	rows, err := db.Query(selectQuery)
	if err != nil {
		return fmt.Errorf("failed to get rows to update: %w", err)
	}
	defer rows.Close()

	var rowIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			d.logger.Warn("Failed to scan row ID", String("table", tableName), Error("error", err))
			continue
		}
		rowIDs = append(rowIDs, id)
	}

	if len(rowIDs) == 0 {
		d.logger.Info("No rows to update", String("table", tableName))
		return nil
	}

	d.logger.Info("Updating rows", String("table", tableName), Int("row_count", len(rowIDs)))

	// Update each row individually
	for _, rowID := range rowIDs {
		if err := d.updateSingleRow(db, tableName, rowID, tableConfig); err != nil {
			d.logger.Warn("Failed to update row", String("table", tableName), Int64("row_id", rowID), Error("error", err))
			continue
		}
	}

	d.logger.Info("Successfully updated rows", String("table", tableName), Int("row_count", len(rowIDs)))
	return nil
}

func (d *DataCleanupService) updateSingleRow(db *sql.DB, tableName string, rowID int64, tableConfig TableUpdateConfig) error {
	// Build the UPDATE query for this specific row
	updates := make([]string, 0)
	args := make([]interface{}, 0)

	for column, fakerType := range tableConfig.Columns {
		// Check if column exists before trying to update it
		exists, err := d.columnExists(db, tableName, column)
		if err != nil {
			d.logger.Warn("Failed to check column", String("table", tableName), String("column", column), Error("error", err))
			continue
		}
		if !exists {
			d.logger.Warn("Column does not exist, skipping", String("table", tableName), String("column", column))
			continue
		}

		fakeValue, err := d.fakeGenerator.GenerateFakeValue(fakerType)
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

	// Add the row ID to the args
	args = append(args, rowID)

	// Execute the UPDATE query for this specific row
	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE id = ?", tableName, strings.Join(updates, ", "))
	
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

func (d *DataCleanupService) columnExists(db *sql.DB, tableName, columnName string) (bool, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s` LIKE '%s'", tableName, columnName)
	rows, err := db.Query(query)
	if err != nil {
		return false, fmt.Errorf("failed to check column %s in table %s: %w", columnName, tableName, err)
	}
	defer rows.Close()
	
	return rows.Next(), nil
}

// GofakeitGenerator implements FakeDataGenerator
type GofakeitGenerator struct{}

func (g *GofakeitGenerator) GenerateFakeValue(fakerType string) (interface{}, error) {
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

func NewZapLogger() *ZapLogger {
	// Create a development logger with caller info and stack traces
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	
	logger, err := config.Build()
	if err != nil {
		// Fallback to standard logger if zap fails
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

	m.logger.Printf("Connected to database: %s", config.DB)

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
	m.logger.Printf("\nTable: %s", config.Table)
	m.logger.Printf("Columns: %v", columns)
	m.logger.Printf("Data:")
	m.logger.Printf("-----")

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
		m.logger.Printf("Row %d:", rowCount)
		for i, col := range columns {
			val := values[i]
			if val == nil {
				m.logger.Printf("  %s: NULL", col)
			} else {
				// Convert byte arrays to strings for better display
				switch v := val.(type) {
				case []byte:
					m.logger.Printf("  %s: %s", col, string(v))
				default:
					m.logger.Printf("  %s: %v", col, val)
				}
			}
		}
		m.logger.Printf("")
	}

	if rowCount == 0 {
		m.logger.Printf("No data found in table.")
	} else {
		m.logger.Printf("Total rows displayed: %d", rowCount)
	}

	return nil
} 