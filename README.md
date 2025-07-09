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

## Priority Order

1. Command line arguments (highest priority)
2. Environment variables
3. Default values (lowest priority)

## Example Output

```
MySQL Cleanup CLI
==================
Host: localhost
User: root
Port: 3306
Password: ********
Config: /path/to/config.json
Database: mydb
Table: mytable
Mode: Single table
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