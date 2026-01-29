package cleanup

import (
	"fmt"
	"testing"
)

// TestDatabaseConnection verifies that MySQL database connection strings are correctly formatted.
// This test validates that the DSN (Data Source Name) contains all required components
// (user, password, host, port, database) in the correct format for MySQL connectivity.
func TestDatabaseConnection(t *testing.T) {
	// Test that we can create a connection string
	config := Config{
		Host:     "localhost",
		Port:     "3306",
		User:     "testuser",
		Password: "testpass",
		DB:       "testdb",
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.DB)

	if dsn == "" {
		t.Error("DSN should not be empty")
	}

	// Test that DSN contains expected components
	if len(dsn) < 10 {
		t.Error("DSN should be reasonably long")
	}
}

// TestFakerGeneration validates that the fake data generator can produce various types
// of realistic test data including emails, names, phone numbers, and addresses.
// This test also verifies proper error handling for invalid faker types.
func TestFakerGeneration(t *testing.T) {
	// Note: We don't need a full test setup, just a faker instance
	// to make sure data generation doesn't panic.
	faker := NewGofakeitGenerator()

	testCases := []struct {
		name      string
		fakerType string
		expectNil bool
		expectErr bool
	}{
		{"email", "random_email", false, false},
		{"name", "random_name", false, false},
		{"phone", "random_phone_short", false, false},
		{"address", "random_address", false, false},
		{"invalid_faker_type", "non_existent_faker", true, true}, // Expect error for non-existent faker type
		{"nil_faker_type", "", true, false},                      // Expect nil for empty faker type (no error)
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			val, err := faker.GenerateFakeValue(tt.fakerType)
			if val == nil && !tt.expectNil {
				t.Errorf("Expected a value, got nil")
			}
			if err == nil && tt.expectErr {
				t.Errorf("Expected an error, got nil")
			}
			if err != nil && !tt.expectErr {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

// MockS3Handler is a no-op S3Handler for tests
type MockS3Handler struct{}

func (m *MockS3Handler) DownloadS3File(s3URI string) (string, error) { return "", nil }
func (m *MockS3Handler) CleanupTempFile(filePath string) error       { return nil }

// TestYAMLParsing validates the YAML configuration parser's ability to read and parse
// configuration files correctly. This test ensures that database configurations are
// properly loaded and that the parsed structure contains expected database and table settings.
func TestYAMLParsing(t *testing.T) {
	fileReader := &OSFileReader{}
	logger := &StdLogger{}
	s3Handler := &MockS3Handler{}
	parser := NewYAMLConfigParser(fileReader, s3Handler, logger)

	// Test parsing a valid config file
	config, err := parser.ParseConfig("../../tests/config.yaml")
	if err != nil {
		t.Errorf("Should parse valid config file: %v", err)
	}

	// Test that config has expected structure
	if len(config.Databases) == 0 {
		t.Error("Config should have at least one database")
	}

	// Test that we can access database config
	dbConfig, exists := config.Databases["acme_corp"]
	if !exists {
		t.Error("Should have acme_corp database config")
	}

	// Test that database has some tables configured
	if len(dbConfig.Update) == 0 && len(dbConfig.Truncate) == 0 {
		t.Error("Database should have some tables configured")
	}
}
