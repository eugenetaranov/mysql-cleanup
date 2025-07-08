package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// fetchAndDisplayTableData connects to the database and fetches data from the specified table
func fetchAndDisplayTableData(config Config) error {
	// Build connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", 
		config.User, config.Password, config.Host, config.Port, config.DB)
	
	// Connect to database
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	fmt.Printf("Connected to database: %s\n", config.DB)

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
	fmt.Printf("\nTable: %s\n", config.Table)
	fmt.Println("Columns:", columns)
	fmt.Println("Data:")
	fmt.Println("-----")

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
		fmt.Printf("Row %d:\n", rowCount)
		for i, col := range columns {
			val := values[i]
			if val == nil {
				fmt.Printf("  %s: NULL\n", col)
			} else {
				// Convert byte arrays to strings for better display
				switch v := val.(type) {
				case []byte:
					fmt.Printf("  %s: %s\n", col, string(v))
				default:
					fmt.Printf("  %s: %v\n", col, val)
				}
			}
		}
		fmt.Println()
	}

	if rowCount == 0 {
		fmt.Println("No data found in table.")
	} else {
		fmt.Printf("Total rows displayed: %d\n", rowCount)
	}

	return nil
} 