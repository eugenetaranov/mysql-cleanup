# MySQL Cleanup CLI

A simple Go CLI application for MySQL cleanup operations with flexible parameter configuration.

## Features

- Command-line argument parsing
- Environment variable support
- `.env` file configuration
- Secure password masking in output

## Installation

1. Clone or download the project
2. Install dependencies:
   ```bash
   go mod tidy
   ```
3. Build the application:
   ```bash
   go build -o bin/mysql_cleanup
   ```

## Usage

### Single Table Mode
Process a specific table from the configuration:

```bash
./bin/mysql_cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -table=mytable
```

### All Tables Mode
Process all tables defined in the configuration:

```bash
./bin/mysql_cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -all-tables
```

### Environment Variables

You can set environment variables:

```bash
export HOST=localhost
export USER=root
export PORT=3306
export PASSWORD=mypass
export DB=mydb
export TABLE=mytable
./bin/mysql_cleanup
```

### .env File

Create a `.env` file in the project directory (see `env.example` for reference):

```bash
cp env.example .env
# Edit .env with your values
./bin/mysql_cleanup
```

## Parameters

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `-host` | `HOST` | `localhost` | Database host |
| `-user` | `USER` | `root` | Database user |
| `-port` | `PORT` | `3306` | Database port |
| `-password` | `PASSWORD` | (empty) | Database password |
| `-config` | `CONFIG` | (empty) | Configuration file path |
| `-db` | `DB` | (empty) | Database name (required) |
| `-table` | `TABLE` | (empty) | Table name (required for single table mode) |
| `-all-tables` | (none) | false | Process all tables (required for all tables mode) |
| `-debug` | (none) | false | Enable debug logging |

## Priority Order

1. Command line arguments (highest priority)
2. Environment variables
3. Default values (lowest priority)

## Logging

The application uses structured logging with different levels:

### Log Levels
- **Info**: Essential information about the operation (default)
- **Debug**: Detailed execution flow and configuration details (use `-debug` flag)
- **Warn**: Warning messages for non-critical issues
- **Error**: Error messages for critical failures

### Debug Mode
Enable detailed logging with the `-debug` flag:

```bash
# Normal operation (Info level and above)
./bin/mysql_cleanup -host=localhost -user=root -db=mydb -table=mytable

# Debug mode (all levels including Debug)
./bin/mysql_cleanup -host=localhost -user=root -db=mydb -table=mytable -debug
```

Debug mode shows:
- Configuration details
- Database connection steps
- SQL query execution
- Row-by-row processing details
- YAML configuration parsing

## Example Output

### Normal Mode (Info level)
```
2025/07/09 03:01:32 Starting MySQL Cleanup CLI...
2025/07/09 03:01:32 Connecting to MySQL database: root@localhost:3306/test
2025-07-09T03:01:32.336+0200    ERROR   Error fetching table data  {"error": "failed to ping database: ..."}
```

### Debug Mode
```
2025/07/09 03:01:35 Starting MySQL Cleanup CLI...
2025-07-09T03:01:35.911+0200    DEBUG   Service created successfully
2025-07-09T03:01:35.911+0200    DEBUG   MySQL Cleanup CLI
2025-07-09T03:01:35.911+0200    DEBUG   Configuration   {"host": "localhost", "user": "root", "port": "3306", "password": "<empty>", "config": "", "database": "test"}
2025-07-09T03:01:35.911+0200    DEBUG   Mode: Single table      {"table": "users"}
2025/07/09 03:01:35 Connecting to MySQL database: root@localhost:3306/test
2025-07-09T03:01:35.915+0200    ERROR   Error fetching table data  {"error": "failed to ping database: ..."}
```

## Testing

This project includes a comprehensive testing infrastructure with both unit tests and integration tests using testcontainers.

### Prerequisites

- Go 1.21+
- Docker Desktop
- Taskfile.dev (optional, for task automation)

### Test Types

#### Unit Tests
Fast, isolated tests that verify individual components without external dependencies.

```bash
# Run unit tests
go test -v

# Run with coverage
go test -v -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html

# Run only short tests
go test -v -short
```

#### Integration Tests
End-to-end tests using testcontainers that spin up real MySQL containers and verify the complete workflow.

```bash
# Run integration tests
go test -v -tags=integration
```

**Note**: Integration tests require Docker to be running.

### Taskfile Commands

The project includes a Taskfile for easy test execution. Navigate to the `tests/` directory and use:

```bash
cd tests/

# Individual test types
task test-unit          # Run unit tests only
task test-integration   # Run integration tests only
task test-all           # Run both unit and integration tests

# Specialized testing
task test-coverage      # Run unit tests with coverage report
task test-short         # Run only short unit tests
task test-debug         # Manual application testing with Docker

# Docker environment management
task up                 # Start MySQL test container
task down               # Stop MySQL test container
task reset              # Reset test databases
task clean              # Clean up everything including volumes
```

### Test Structure

```
tests/
├── Taskfile.yml           # Task definitions for testing
├── config.yaml            # Test configuration
├── docker-compose.yml     # MySQL test environment
└── mysql/
    └── init/              # Database initialization scripts
        ├── 01-schema.sql  # Database schema
        └── 02-data.sql    # Sample test data
```

## Test Suite Overview

The project includes a comprehensive test suite with 14 test functions covering unit tests, integration tests, and edge cases:

| Test Name | Type | File | Purpose/Scope |
|-----------|------|------|---------------|
| **TestDatabaseConnection** | Unit | `implementations_test.go` | Verifies MySQL DSN (Data Source Name) formatting and connection string validation |
| **TestFakerGeneration** | Unit | `implementations_test.go` | Validates fake data generation for emails, names, phones, addresses, and error handling |
| **TestYAMLParsing** | Unit | `implementations_test.go` | Validates YAML configuration parser functionality and error handling |
| **TestSimpleContainer** | Basic | `simple_test.go` | Basic MySQL database connectivity test using test containers |
| **TestMySQLContainerSeedAndQuery** | Integration | `integration_test.go` | Verifies MySQL container startup, data seeding, and query execution |
| **TestFakerDataChanges** | Integration | `integration_test.go` | End-to-end data cleanup workflow testing with database verification |
| **TestParallelWorkers** | Integration | `integration_test.go` | Validates multi-worker parallel processing and concurrency handling |
| **TestLargeBatches** | Integration | `integration_test.go` | Tests handling of large batch sizes without memory issues |
| **TestPerformanceComparison** | Integration | `integration_test.go` | Benchmarks single-threaded vs multi-threaded processing performance |
| **TestErrorHandlingInParallel** | Integration | `integration_test.go` | Validates error handling when multiple workers encounter issues |
| **TestEdgeCaseZeroRows** | Integration | `integration_test.go` | Handles empty tables gracefully without errors or crashes |
| **TestEdgeCaseBatchLargerThanRows** | Integration | `integration_test.go` | Handles cases where batch size exceeds total table rows |
| **TestEdgeCaseBatchSizeOne** | Integration | `integration_test.go` | Validates minimum batch size behavior and single-row processing |
| **TestNonIdPrimaryKey** | Integration | `integration_test.go` | Handles tables with non-standard primary key column names |
| **TestCompositePrimaryKey** | Integration | `integration_test.go` | Handles tables with composite primary keys (multiple columns) |
| **TestS3ConfigDownload** | Integration | `integration_test.go` | Validates S3 configuration file downloading and parsing |

### Test Categories

#### 🔧 **Unit Tests (3 tests)**
Fast, isolated tests that verify individual components without external dependencies.
- Database connection validation
- Fake data generation logic
- Configuration parsing

#### 🐳 **Container Tests (1 test)**
Basic connectivity tests using Docker containers.
- MySQL container setup and connectivity

#### 🔄 **Integration Tests (11 tests)**
End-to-end tests using testcontainers with real MySQL instances.
- Complete workflow validation
- Performance benchmarking  
- Error handling verification
- Edge case coverage
- Primary key variations
- External configuration sources

### Integration Test Details

Integration tests use testcontainers-go to:

1. **Spin up MySQL containers** with the test schema and data
2. **Verify database connectivity** and seeding
3. **Test faker data generation** and database updates
4. **Compare data changes** between original and faked databases
5. **Clean up containers** automatically

The tests verify:
- Database connection and query execution
- YAML configuration parsing
- Faker data generation for emails, names, phones, addresses
- End-to-end cleanup workflow with real MySQL data

### Test Configuration

Tests use a `config.yaml` file that defines:
- Database tables and their cleanup rules
- Faker field mappings
- Exclude clauses for sensitive data
- Truncate rules for audit logs

### Running Tests in CI/CD

For continuous integration, use:

```bash
# Install dependencies
go mod download

# Run all tests
cd tests && task test-all

# Or run directly
go test -v ./...
go test -v -tags=integration ./...
```

### Test Environment Variables

Integration tests use these environment variables:
- `TESTCONTAINERS_RYUK_DISABLED=true` - Disable Ryuk container for faster cleanup

### Troubleshooting

**Docker not running**: Integration tests require Docker Desktop to be running.

**Port conflicts**: Tests use port 3306 for MySQL. Ensure no other MySQL instance is running.

**Container cleanup**: If containers don't clean up properly, run `task clean` to force cleanup.

**Test failures**: Check Docker logs with `task logs` to debug container issues. 