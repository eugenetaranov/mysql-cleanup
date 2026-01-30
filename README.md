# MySQL Cleanup CLI

A simple Go CLI application for MySQL cleanup operations with flexible parameter configuration.

## Features

- Command-line argument parsing
- Environment variable support
- `.env` file configuration
- Secure password masking in output

## Installation

### Option 1: Docker (Recommended)

```bash
# Pull the latest image
docker pull eugenetaranov/mysql-cleanup:latest

# Run with Docker
docker run --rm eugenetaranov/mysql-cleanup:latest --help
```

For detailed Docker usage, see [DOCKER.md](DOCKER.md).

### Option 2: Build from Source

1. Clone or download the project
2. Install dependencies:
   ```bash
   go mod tidy
   ```
3. Build the application:
   ```bash
   go build -o bin/mysql-cleanup
   ```

## Usage

### Single Table Mode
Process a specific table from the configuration:

```bash
./bin/mysql-cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -table=mytable
```

### Multiple Tables Mode
Process multiple specific tables from the configuration:

```bash
./bin/mysql-cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -table=table1 -table=table2 -table=table3
```

### All Tables Mode
Process all tables defined in the configuration:

```bash
./bin/mysql-cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -all-tables
```

### Tables Without Primary Keys

The tool automatically detects tables without primary keys and uses offset-based processing:

```bash
# For junction/mapping tables without primary keys
./bin/mysql-cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -table=email_history_to
```

**Features for tables without primary keys:**
- Automatic detection and offset-based processing
- Parallel batch processing with `LIMIT`/`OFFSET`
- Progress tracking and ETA calculation
- Exclude clause support
- **Note**: Range filtering is not supported for tables without primary keys

### Environment Variables

You can set environment variables:

```bash
export HOST=localhost
export USER=root
export PORT=3306
export PASSWORD=mypass
export DB=mydb
export TABLE=mytable
./bin/mysql-cleanup
```

### .env File

Create a `.env` file in the project directory (see `env.example` for reference):

```bash
cp env.example .env
# Edit .env with your values
./bin/mysql-cleanup
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
| `-table` | `TABLE` | (empty) | Table name (can be specified multiple times for multiple table mode) |
| `-all-tables` | (none) | false | Process all tables (required for all tables mode) |
| `-debug` | (none) | false | Enable debug logging |
| `-workers` | (none) | 1 | Number of worker goroutines for parallel processing |
| `-batch-size` | (none) | "1" | Batch size for updates (e.g., "1", "1K", "10K", "100K" - supports K/M/B suffixes) |
| `-range` | (none) | (empty) | ID range to process (e.g., '0:1000', '1000:', ':100K', '100K:1M'; colon required; supports K/M/B suffixes) |
| `-log-file` | (none) | (empty) | Log file path for saving logs (optional) |

## Range Filtering

You can limit processing to a specific range of primary key IDs using the `-range` parameter. This is useful for partial processing, resuming, or parallelizing work.

**Syntax:**
- `-range 0:1000` — Process IDs 0 to 1000 (inclusive)
- `-range 1000:` — Process IDs 1000 and above
- `-range :1000` — Process IDs up to 1000
- `-range 100K:1M` — Process IDs 100,000 to 1,000,000
- `-range :1M` — Process IDs up to 1,000,000
- `-range 1M:` — Process IDs 1,000,000 and above

**Notes:**
- The colon (`:`) is required in all range specifications.
- You can use `K` (thousand), `M` (million), or `B` (billion) suffixes (case-insensitive).
- If `-range` is not specified, all rows are processed.

**Examples:**
```bash
# Process only IDs 1 to 1000
./bin/mysql-cleanup -db=mydb -table=mytable -range 1:1000

# Process IDs 100,000 to 1,000,000
./bin/mysql-cleanup -db=mydb -table=mytable -range 100K:1M

# Process IDs from 1,000,000 and up
./bin/mysql-cleanup -db=mydb -table=mytable -range 1M:

# Process all rows (no range specified)
./bin/mysql-cleanup -db=mydb -table=mytable
```

## Humanized Batch Sizes

You can specify batch sizes using humanized formats for easier configuration of large batch operations.

**Syntax:**
- `-batch-size 1` — Process 1 row per batch
- `-batch-size 100` — Process 100 rows per batch
- `-batch-size 1K` — Process 1,000 rows per batch
- `-batch-size 10K` — Process 10,000 rows per batch
- `-batch-size 100K` — Process 100,000 rows per batch
- `-batch-size 1M` — Process 1,000,000 rows per batch

**Notes:**
- You can use `K` (thousand), `M` (million), or `B` (billion) suffixes (case-insensitive).
- The batch size must be positive.
- Larger batch sizes can improve performance but use more memory.

**Examples:**
```bash
# Small batches for testing
./bin/mysql-cleanup -db=mydb -table=mytable -batch-size 10

# Medium batches for production
./bin/mysql-cleanup -db=mydb -table=mytable -batch-size 1K

# Large batches for high-performance processing
./bin/mysql-cleanup -db=mydb -table=mytable -batch-size 10K

# Very large batches for massive datasets
./bin/mysql-cleanup -db=mydb -table=mytable -batch-size 100K
```

## Configuration File Format

The application uses YAML configuration files to define table update rules. The configuration supports both random data generation and static values.

### Table Requirements

The tool supports two types of tables:

1. **Tables with Primary Keys** (Recommended): Uses primary key-based batch processing for optimal performance
2. **Tables without Primary Keys**: Uses offset-based processing with `LIMIT`/`OFFSET` for junction/mapping tables

**Note**: Range filtering (`-range` parameter) is only supported for tables with primary keys.

### Configuration Structure

```yaml
databases:
  database_name:
    truncate:
      - table_to_truncate
    
    update:
      table_name:
        columns:
          column_name: faker_type_or_static_value
        exclude_clause: "SQL WHERE clause"
```

### Supported Faker Types

The following faker types are supported for generating random data:

- `random_email` - Random email address
- `random_name` - Random full name
- `random_firstname` - Random first name
- `random_lastname` - Random last name
- `random_company` - Random company name
- `random_address` - Random street address
- `random_city` - Random city name
- `random_state` - Random state abbreviation
- `random_country_code` - Random country code
- `random_postalcode` - Random postal code
- `random_phone_short` - Random phone number (short format)
- `random_username` - Random username
- `random_id` - Random UUID
- `random_text` - Random text (1-3 sentences)
- `random_word` - Random word
- `random_email_subject` - Random email subject
- `random_file_name` - Random filename with extension
- `random_number_txt` - Random number as text
- `random_room_number_txt` - Random room number as text

### Static Values

You can specify static values using the `static_value:` prefix:

```yaml
columns:
  # Static integer
  status: "static_value: 1"
  
  # Static boolean
  is_active: "static_value: true"
  
  # Static string
  category: "static_value: premium"
  
  # Static NULL
  optional_field: "static_value: NULL"
  
  # Static float
  price: "static_value: 99.99"
```

**Supported static value types:**
- **Integers**: `"static_value: 1"`, `"static_value: 42"`
- **Floats**: `"static_value: 3.14"`, `"static_value: 99.99"`
- **Booleans**: `"static_value: true"`, `"static_value: false"`
- **NULL**: `"static_value: NULL"`, `"static_value: null"`
- **Strings**: `"static_value: hello"`, `"static_value: test string"`

### Configuration Examples

**Example 1: Basic table with random data**
```yaml
databases:
  mydb:
    update:
      users:
        columns:
          email: random_email
          name: random_name
          phone: random_phone_short
        exclude_clause: "email RLIKE '.*@company\\.com'"
```

**Example 2: Table with mixed random and static values**
```yaml
databases:
  mydb:
    update:
      products:
        columns:
          name: random_word
          price: "static_value: 99.99"
          status: "static_value: 1"
          category: "static_value: active"
          description: random_text
        exclude_clause: "status = 0"
```

**Example 3: Table with NULL values**
```yaml
databases:
  mydb:
    update:
      orders:
        columns:
          customer_email: random_email
          notes: "static_value: NULL"
          priority: "static_value: 0"
          status: "static_value: pending"
        exclude_clause: "customer_email RLIKE '.*@internal\\.com'"
```

## Priority Order

1. Command line arguments (highest priority)
2. Environment variables
3. Default values (lowest priority)

## Performance Configuration

The application supports parallel processing with configurable workers and batch sizes for optimal performance.

### Performance Parameters

- **Workers**: Number of concurrent goroutines processing data in parallel
- **Batch Size**: Number of rows processed in a single database operation

### Performance Examples

```bash
# Single-threaded processing (default)
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable

# Multi-threaded with 4 workers and small batches
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -workers=4 -batch-size=10

# High-performance with large batches
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -workers=4 -batch-size=1K

# Very high-performance with very large batches
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -workers=8 -batch-size=10K
```

### Performance Optimization

- **Small datasets**: Use default settings (1 worker, 1 batch size)
- **Medium datasets**: Use 2-4 workers with batch sizes of 10-50
- **Large datasets**: Use 4-8 workers with batch sizes of 50-200
- **Very large datasets**: Use 8+ workers with batch sizes of 100-500

The application uses bulk `INSERT ... ON DUPLICATE KEY UPDATE` operations which provide significant performance improvements over row-by-row updates.

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
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable

# Debug mode (all levels including Debug)
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -debug
```

Debug mode shows:
- Configuration details
- Database connection steps
- SQL query execution
- Row-by-row processing details
- YAML configuration parsing

### Log File Output

You can save logs to a file in addition to console output using the `-log-file` parameter:

```bash
# Save logs to a file
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -log-file=cleanup.log

# Save logs with debug mode
./bin/mysql-cleanup -host=localhost -user=root -db=mydb -table=mytable -debug -log-file=debug.log
```

**Benefits of log files:**
- Persistent record of operations for auditing
- Easier troubleshooting of long-running processes
- Can be analyzed later for performance metrics
- Useful for batch processing and automation

**Log file format:**
- Contains all console output including timestamps
- Preserves log levels (Info, Debug, Warn, Error)
- Includes progress updates and ETA calculations
- Can be used with log analysis tools

**Note:** Log files are appended to, not overwritten. Each run adds to the existing file, preserving the history of all operations. To start fresh, delete the log file before running or use a different filename.

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

## Troubleshooting

### Common Issues

**"Table structure issue - the specified table does not have a primary key defined"**

This error occurs when trying to process a table without a primary key using the old version of the tool. The new version automatically handles tables without primary keys using offset-based processing.

**"Range filtering is not supported for tables without primary keys"**

Range filtering (`-range` parameter) requires a primary key to efficiently filter rows. For tables without primary keys, process the entire table or use exclude clauses in the configuration.

**"Failed to generate fake value - unknown faker type: static_value: X"**

This error occurs when using static values with an older version of the tool. The new version supports static values with the `static_value:` prefix.

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