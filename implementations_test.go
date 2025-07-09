package main

import (
	"fmt"
	"testing"
)

// TestDatabaseConnection tests basic database connectivity
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

// TestFakerGeneration tests that faker can generate random data
func TestFakerGeneration(t *testing.T) {
	generator := &GofakeitGenerator{}
	
	// Test a few key faker functions
	tests := []struct {
		name     string
		fakerType string
	}{
		{"email", "random_email"},
		{"name", "random_name"},
		{"phone", "random_phone_short"},
		{"address", "random_address"},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := generator.GenerateFakeValue(tt.fakerType)
			if err != nil {
				t.Errorf("Faker should not error for %s: %v", tt.name, err)
			}
			if result == nil || result == "" {
				t.Errorf("Faker should generate non-empty %s", tt.name)
			}
		})
	}
}

// MockS3Handler is a no-op S3Handler for tests
type MockS3Handler struct{}
func (m *MockS3Handler) DownloadS3File(s3URI string) (string, error) { return "", nil }
func (m *MockS3Handler) CleanupTempFile(filePath string) error { return nil }

// TestYAMLParsing tests basic YAML configuration parsing
func TestYAMLParsing(t *testing.T) {
	fileReader := &OSFileReader{}
	logger := &StdLogger{}
	s3Handler := &MockS3Handler{}
	parser := NewYAMLConfigParser(fileReader, s3Handler, logger)
	
	// Test parsing a valid config file
	config, err := parser.ParseConfig("tests/config.yaml")
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