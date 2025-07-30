package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestSimpleContainer verifies basic MySQL database connectivity using a test container.
// This test creates a minimal MySQL container, connects to it, and executes a simple
// "SELECT 1" query to ensure the database connection and basic functionality works.
func TestSimpleContainer(t *testing.T) {
	container, db, cleanup := setupSimpleTestMySQLContainer(t)
	defer container.Terminate(context.Background())
	defer cleanup()

	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("failed to query database: %s", err)
	}
	if result != 1 {
		t.Errorf("expected 1, got %d", result)
	}
}

// setupSimpleTestMySQLContainer creates a basic MySQL container for simple testing
func setupSimpleTestMySQLContainer(t *testing.T) (testcontainers.Container, *sql.DB, func()) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "mysql:8.0.36",
		ExposedPorts: []string{"3306/tcp"},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "root",
			"MYSQL_DATABASE":      "testdb",
		},
		WaitingFor: wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(30 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("Failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		t.Fatalf("Failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("root:root@tcp(%s:%s)/testdb?parseTime=true", host, port.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Failed to open database connection: %v", err)
	}

	// Ping the database to ensure it's ready
	if err := db.Ping(); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Logf("Failed to close db connection: %v", err)
		}
	}

	return container, db, cleanup
}

func TestParseHumanizedRange(t *testing.T) {
	tests := []struct {
		input    string
		expected *IDRange
		hasError bool
	}{
		// Valid humanized ranges
		{":100K", &IDRange{Start: nil, End: int64Ptr(100000), HasRange: true}, false},
		{"100K:", &IDRange{Start: int64Ptr(100000), End: nil, HasRange: true}, false},
		{"100K:1M", &IDRange{Start: int64Ptr(100000), End: int64Ptr(1000000), HasRange: true}, false},
		{"1M:2M", &IDRange{Start: int64Ptr(1000000), End: int64Ptr(2000000), HasRange: true}, false},
		{"500K:1M", &IDRange{Start: int64Ptr(500000), End: int64Ptr(1000000), HasRange: true}, false},
		{":1B", &IDRange{Start: nil, End: int64Ptr(1000000000), HasRange: true}, false},
		{"1B:", &IDRange{Start: int64Ptr(1000000000), End: nil, HasRange: true}, false},

		// Mixed formats
		{"1000:100K", &IDRange{Start: int64Ptr(1000), End: int64Ptr(100000), HasRange: true}, false},
		{"100K:1000000", &IDRange{Start: int64Ptr(100000), End: int64Ptr(1000000), HasRange: true}, false},

		// Case insensitive
		{":100k", &IDRange{Start: nil, End: int64Ptr(100000), HasRange: true}, false},
		{":1m", &IDRange{Start: nil, End: int64Ptr(1000000), HasRange: true}, false},
		{":1b", &IDRange{Start: nil, End: int64Ptr(1000000000), HasRange: true}, false},

		// Invalid formats
		{"1000", nil, true}, // No colon
		{"100K", nil, true}, // No colon
		{":", &IDRange{Start: nil, End: nil, HasRange: true}, false}, // Empty start and end
		{"100K:invalid", nil, true},                                  // Invalid end value
		{"invalid:100K", nil, true},                                  // Invalid start value
	}

	for _, test := range tests {
		result, err := ParseIDRange(test.input)

		if test.hasError {
			if err == nil {
				t.Errorf("Expected error for input '%s', but got none", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for input '%s': %v", test.input, err)
			} else if !rangesEqual(result, test.expected) {
				t.Errorf("For input '%s', expected %v, got %v", test.input, test.expected, result)
			}
		}
	}
}

func TestParseHumanizedBatchSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		// Valid humanized batch sizes
		{"1", 1, false},
		{"100", 100, false},
		{"1K", 1000, false},
		{"10K", 10000, false},
		{"100K", 100000, false},
		{"1M", 1000000, false},
		{"10M", 10000000, false},
		{"1B", 1000000000, false},
		{"10B", 10000000000, false},

		// Case insensitive
		{"1k", 1000, false},
		{"1m", 1000000, false},
		{"1b", 1000000000, false},

		// Edge cases
		{"0", 0, true},       // Must be positive
		{"-1", 0, true},      // Must be positive
		{"", 0, true},        // Empty
		{"invalid", 0, true}, // Invalid format
		{"1K2", 0, true},     // Invalid format
		{"K1", 0, true},      // Invalid format
	}

	for _, test := range tests {
		result, err := parseHumanizedBatchSize(test.input)

		if test.hasError {
			if err == nil {
				t.Errorf("Expected error for input '%s', but got none", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for input '%s': %v", test.input, err)
			} else if result != test.expected {
				t.Errorf("For input '%s', expected %d, got %d", test.input, test.expected, result)
			}
		}
	}
}

func TestSingleTableModeChecksBothSections(t *testing.T) {
	// Create a test config with both update and truncate sections
	config := Config{
		DB:     "testdb",
		Tables: []string{"users"},
		Config: "tests/config.yaml",
	}

	// Create service
	service := createService(false, 1, "1K", "")

	// Parse config to verify the new behavior
	err := service.configParser.ParseAndDisplayConfigFiltered(config.Config, config)
	if err != nil {
		t.Fatalf("Failed to parse config: %v", err)
	}

	// The test passes if no error is returned, indicating the table was found
	// in either update or truncate section
}

func int64Ptr(val int64) *int64 {
	return &val
}

func rangesEqual(a, b *IDRange) bool {
	if a.HasRange != b.HasRange {
		return false
	}

	if a.Start == nil && b.Start != nil {
		return false
	}
	if a.Start != nil && b.Start == nil {
		return false
	}
	if a.Start != nil && b.Start != nil && *a.Start != *b.Start {
		return false
	}

	if a.End == nil && b.End != nil {
		return false
	}
	if a.End != nil && b.End == nil {
		return false
	}
	if a.End != nil && b.End != nil && *a.End != *b.End {
		return false
	}

	return true
}

func TestStaticValueGeneration(t *testing.T) {
	// Create a schema-aware generator
	logger := &StdLogger{}
	generator := NewSchemaAwareGofakeitGenerator(logger)

	// Test cases for static values
	testCases := []struct {
		input    string
		expected interface{}
		hasError bool
	}{
		{"static_value: 1", 1, false},
		{"static_value: 42", 42, false},
		{"static_value: 3.14", 3.14, false},
		{"static_value: true", true, false},
		{"static_value: false", false, false},
		{"static_value: NULL", nil, false},
		{"static_value: null", nil, false},
		{"static_value: hello", "hello", false},
		{"static_value: test string", "test string", false},
		{"static_value: ", "", false},
		{"static_value: 0", 0, false},
		{"static_value: -1", -1, false},
		{"static_value: 0.0", 0.0, false},
		{"static_value: -3.14", -3.14, false},
		{"static_value: TRUE", true, false},
		{"static_value: FALSE", false, false},
		{"static_value: True", true, false},
		{"static_value: False", false, false},
		{"", nil, false}, // Empty string
		// Test that non-static values still work as before
		{"random_email", nil, false}, // Should not be treated as static value
		{"random_name", nil, false},  // Should not be treated as static value
		{"unknown_type", nil, true},  // Should return error for unknown types
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := generator.generateBasicFakeValue(tc.input)

			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error for input '%s', but got none", tc.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input '%s': %v", tc.input, err)
				} else if result != tc.expected {
					// For random faker types, we don't know the exact value, just check it's not nil
					if strings.HasPrefix(tc.input, "random_") && tc.expected == nil {
						if result == nil {
							t.Errorf("For input '%s', expected non-nil result, got nil", tc.input)
						}
					} else if result != tc.expected {
						t.Errorf("For input '%s', expected %v, got %v", tc.input, tc.expected, result)
					}
				}
			}
		})
	}
}

func TestStaticValueGenerationBasic(t *testing.T) {
	// Create a basic generator
	generator := NewGofakeitGenerator()

	// Test cases for static values
	testCases := []struct {
		input    string
		expected interface{}
		hasError bool
	}{
		{"static_value: 1", 1, false},
		{"static_value: 42", 42, false},
		{"static_value: 3.14", 3.14, false},
		{"static_value: true", true, false},
		{"static_value: false", false, false},
		{"static_value: NULL", nil, false},
		{"static_value: null", nil, false},
		{"static_value: hello", "hello", false},
		{"static_value: test string", "test string", false},
		{"static_value: ", "", false},
		{"static_value: 0", 0, false},
		{"static_value: -1", -1, false},
		{"static_value: 0.0", 0.0, false},
		{"static_value: -3.14", -3.14, false},
		{"static_value: TRUE", true, false},
		{"static_value: FALSE", false, false},
		{"static_value: True", true, false},
		{"static_value: False", false, false},
		{"", nil, false}, // Empty string
		// Test that non-static values still work as before
		{"random_email", nil, false}, // Should not be treated as static value
		{"random_name", nil, false},  // Should not be treated as static value
		{"unknown_type", nil, true},  // Should return error for unknown types
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result, err := generator.GenerateFakeValue(tc.input)

			if tc.hasError {
				if err == nil {
					t.Errorf("Expected error for input '%s', but got none", tc.input)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for input '%s': %v", tc.input, err)
				} else if result != tc.expected {
					// For random faker types, we don't know the exact value, just check it's not nil
					if strings.HasPrefix(tc.input, "random_") && tc.expected == nil {
						if result == nil {
							t.Errorf("For input '%s', expected non-nil result, got nil", tc.input)
						}
					} else if result != tc.expected {
						t.Errorf("For input '%s', expected %v, got %v", tc.input, tc.expected, result)
					}
				}
			}
		})
	}
}

func TestStaticValueErrorHandling(t *testing.T) {
	// Test that the original error case is properly handled
	logger := &StdLogger{}
	generator := NewSchemaAwareGofakeitGenerator(logger)

	// Test the specific case that was failing before
	result, err := generator.generateBasicFakeValue("static_value: 1")
	if err != nil {
		t.Errorf("Expected no error for 'static_value: 1', but got: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected 1 for 'static_value: 1', but got: %v", result)
	}

	// Test that unknown types still return errors
	_, err = generator.generateBasicFakeValue("unknown_faker_type")
	if err == nil {
		t.Error("Expected error for unknown faker type, but got none")
	}
	if !strings.Contains(err.Error(), "unknown faker type") {
		t.Errorf("Expected error message to contain 'unknown faker type', but got: %v", err)
	}

	// Test that regular faker types still work
	result, err = generator.generateBasicFakeValue("random_email")
	if err != nil {
		t.Errorf("Expected no error for 'random_email', but got: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result for 'random_email'")
	}
}

func TestStaticValueErrorHandlingBasic(t *testing.T) {
	// Test that the original error case is properly handled in basic generator
	generator := NewGofakeitGenerator()

	// Test the specific case that was failing before
	result, err := generator.GenerateFakeValue("static_value: 1")
	if err != nil {
		t.Errorf("Expected no error for 'static_value: 1', but got: %v", err)
	}
	if result != 1 {
		t.Errorf("Expected 1 for 'static_value: 1', but got: %v", result)
	}

	// Test that unknown types still return errors
	_, err = generator.GenerateFakeValue("unknown_faker_type")
	if err == nil {
		t.Error("Expected error for unknown faker type, but got none")
	}
	if !strings.Contains(err.Error(), "unknown faker type") {
		t.Errorf("Expected error message to contain 'unknown faker type', but got: %v", err)
	}

	// Test that regular faker types still work
	result, err = generator.GenerateFakeValue("random_email")
	if err != nil {
		t.Errorf("Expected no error for 'random_email', but got: %v", err)
	}
	if result == nil {
		t.Error("Expected non-nil result for 'random_email'")
	}
}
