# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.1] - 2024-07-24

### Added
- Initial release of MySQL Cleanup tool
- Support for cleaning up PII data in MySQL databases
- Multi-architecture Docker support (Linux/macOS, AMD64/ARM64)
- YAML configuration file support
- S3 configuration file download support
- Parallel batch processing with configurable workers
- Support for composite primary keys
- Range-based data processing (e.g., "1K:10K", ":100K")
- Humanized batch sizes (e.g., "1K", "10K", "100K")
- Comprehensive logging with file and console output
- Integration tests with testcontainers
- CI/CD pipeline with GitHub Actions
- Automatic binary releases for multiple platforms
- Docker image releases with multi-architecture support

### Features
- **Data Anonymization**: Replace sensitive data with fake data using gofakeit
- **Batch Processing**: Process large datasets efficiently with parallel workers
- **Schema Awareness**: Automatically detect column types and generate appropriate fake data
- **Flexible Configuration**: Support for YAML config files and S3 storage
- **Range Processing**: Process specific ID ranges for targeted cleanup
- **Comprehensive Testing**: Unit tests, integration tests, and performance tests
- **Production Ready**: Proper error handling, logging, and monitoring

### Technical Details
- Built with Go 1.22+
- Supports MySQL 8.0+
- Multi-platform binaries (Linux, macOS, Windows)
- Docker images for easy deployment
- MIT License 