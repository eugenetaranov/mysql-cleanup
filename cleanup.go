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
	"gopkg.in/yaml.v3"
)

// Global counter for unique identifiers
var uniqueCounter int64

// CleanupData performs the actual data cleanup based on the configuration
func CleanupData(config Config) error {
	// Parse the YAML configuration
	yamlConfig, err := parseConfig(config.Config)
	if err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	// Connect to database
	db, err := connectToDatabase(config)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Process each database in the config
	for dbName, dbConfig := range yamlConfig.Databases {
		if dbName != config.DB {
			continue // Skip if not the target database
		}

		log.Printf("Processing database: %s", dbName)

		// Handle truncate tables
		if len(dbConfig.Truncate) > 0 {
			if err := truncateTables(db, dbConfig.Truncate); err != nil {
				log.Printf("Warning: failed to truncate tables: %v", err)
			}
		}

		// Handle update tables
		if len(dbConfig.Update) > 0 {
			if err := updateTables(db, dbConfig.Update); err != nil {
				log.Printf("Warning: failed to update tables: %v", err)
			}
		}
	}

	return nil
}

// parseConfig reads and parses the YAML configuration file
func parseConfig(configPath string) (*YAMLConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var yamlConfig YAMLConfig
	if err := yaml.Unmarshal(data, &yamlConfig); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return &yamlConfig, nil
}

// connectToDatabase establishes a connection to the MySQL database
func connectToDatabase(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", 
		config.User, config.Password, config.Host, config.Port, config.DB)
	
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// truncateTables truncates the specified tables
func truncateTables(db *sql.DB, tables []string) error {
	for _, table := range tables {
		log.Printf("Truncating table: %s", table)
		query := fmt.Sprintf("TRUNCATE TABLE `%s`", table)
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("failed to truncate table %s: %w", table, err)
		}
		log.Printf("Successfully truncated table: %s", table)
	}
	return nil
}

// updateTables updates the specified tables with faker data
func updateTables(db *sql.DB, tableConfigs map[string]TableUpdateConfig) error {
	for tableName, tableConfig := range tableConfigs {
		log.Printf("Updating table: %s", tableName)
		
		if err := updateTableData(db, tableName, tableConfig); err != nil {
			return fmt.Errorf("failed to update table %s: %w", tableName, err)
		}
		
		log.Printf("Successfully updated table: %s", tableName)
	}
	return nil
}

// columnExists checks if a column exists in the specified table
func columnExists(db *sql.DB, tableName, columnName string) (bool, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s` LIKE '%s'", tableName, columnName)
	rows, err := db.Query(query)
	if err != nil {
		return false, fmt.Errorf("failed to check column %s in table %s: %w", columnName, tableName, err)
	}
	defer rows.Close()
	
	return rows.Next(), nil
}

// updateTableData updates a single table with faker data
func updateTableData(db *sql.DB, tableName string, tableConfig TableUpdateConfig) error {
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
			log.Printf("Warning: failed to scan row ID: %v", err)
			continue
		}
		rowIDs = append(rowIDs, id)
	}

	if len(rowIDs) == 0 {
		log.Printf("No rows to update in table: %s", tableName)
		return nil
	}

	log.Printf("Updating %d rows in table: %s", len(rowIDs), tableName)

	// Update each row individually
	for _, rowID := range rowIDs {
		if err := updateSingleRow(db, tableName, rowID, tableConfig); err != nil {
			log.Printf("Warning: failed to update row %d in table %s: %v", rowID, tableName, err)
			continue
		}
	}

	log.Printf("Successfully updated %d rows in table: %s", len(rowIDs), tableName)
	return nil
}

// updateSingleRow updates a single row with unique faker data
func updateSingleRow(db *sql.DB, tableName string, rowID int64, tableConfig TableUpdateConfig) error {
	// Build the UPDATE query for this specific row
	updates := make([]string, 0)
	args := make([]interface{}, 0)

	for column, fakerType := range tableConfig.Columns {
		// Check if column exists before trying to update it
		exists, err := columnExists(db, tableName, column)
		if err != nil {
			log.Printf("Warning: failed to check column %s in table %s: %v", column, tableName, err)
			continue
		}
		if !exists {
			log.Printf("Warning: column %s does not exist in table %s, skipping", column, tableName)
			continue
		}

		fakeValue, err := generateFakeValue(fakerType)
		if err != nil {
			log.Printf("Warning: failed to generate fake value for %s.%s: %v", tableName, column, err)
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
		log.Printf("Warning: no rows affected for row ID %d in table %s", rowID, tableName)
	}

	return nil
}

// generateFakeValue generates a fake value based on the specified type
func generateFakeValue(fakerType string) (interface{}, error) {
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