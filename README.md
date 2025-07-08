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

### Command Line Arguments

```bash
./bin/mysql_cleanup -host=localhost -user=root -port=3306 -password=mypass -db=mydb -table=mytable
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
| `-db` | `DB` | (empty) | Database name |
| `-table` | `TABLE` | (empty) | Table name |

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
``` 