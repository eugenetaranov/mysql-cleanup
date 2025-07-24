# Docker Multi-Architecture Build

This project supports building Docker images for multiple architectures including Linux (AMD64/ARM64) and macOS (AMD64/ARM64).

## Quick Start

### Using the pre-built image

```bash
# Pull and run the latest version
docker run --rm your-dockerhub-username/mysql-cleanup:latest --help

# Run with your configuration
docker run --rm \
  -v $(pwd)/config.yaml:/app/config.yaml \
  your-dockerhub-username/mysql-cleanup:latest \
  --host localhost \
  --port 3306 \
  --user root \
  --password yourpassword \
  --db yourdatabase \
  --config /app/config.yaml \
  --table yourtable
```

## Building Locally

### Prerequisites

- Docker with Buildx support
- Docker Hub account (for pushing)

### Manual Build

```bash
# Build for multiple architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag your-dockerhub-username/mysql-cleanup:latest \
  --push \
  .

# Build for local use (macOS)
docker buildx build \
  --platform darwin/amd64,darwin/arm64 \
  --tag mysql-cleanup:local \
  --load \
  .
```

### Using the Build Script

```bash
# Build and push to Docker Hub
./build-multiarch.sh v1.0.0

# Build for local use only
./build-multiarch.sh local
```

## Supported Architectures

- **Linux AMD64**: Standard x86_64 servers and desktops
- **Linux ARM64**: ARM-based servers (AWS Graviton, Apple M1/M2 in Linux)
- **macOS AMD64**: Intel-based Macs
- **macOS ARM64**: Apple Silicon (M1/M2/M3) Macs

## GitHub Actions

The project includes a GitHub Actions workflow that automatically builds and pushes multi-architecture images on:

- Push to main/master branch
- Tag releases (v*)
- Pull requests (build only, no push)

### Setup GitHub Secrets

Add these secrets to your GitHub repository:

- `DOCKER_USERNAME`: Your Docker Hub username
- `DOCKER_PASSWORD`: Your Docker Hub password or access token

## Usage Examples

### Basic Usage

```bash
docker run --rm your-dockerhub-username/mysql-cleanup:latest \
  --host mysql.example.com \
  --port 3306 \
  --user myuser \
  --password mypassword \
  --db mydatabase \
  --table users \
  --config config.yaml
```

### With Volume Mounts

```bash
docker run --rm \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/logs:/app/logs \
  your-dockerhub-username/mysql-cleanup:latest \
  --host mysql.example.com \
  --port 3306 \
  --user myuser \
  --password mypassword \
  --db mydatabase \
  --config /app/config/config.yaml \
  --log-file /app/logs/cleanup.log
```

### With Network Access

```bash
docker run --rm \
  --network host \
  your-dockerhub-username/mysql-cleanup:latest \
  --host localhost \
  --port 3306 \
  --user root \
  --password root \
  --db testdb \
  --table users
```

## Development

### Building for Development

```bash
# Build for your local architecture
docker build -t mysql-cleanup:dev .

# Run with development config
docker run --rm mysql-cleanup:dev --help
```

### Testing Multi-Arch Builds

```bash
# Test all architectures locally
docker buildx build \
  --platform linux/amd64,linux/arm64,darwin/amd64,darwin/arm64 \
  --tag mysql-cleanup:test \
  --load \
  .
```

## Troubleshooting

### Build Issues

1. **Buildx not available**: Update Docker to latest version
2. **QEMU errors**: Install QEMU for cross-platform builds
3. **Memory issues**: Increase Docker memory limit

### Runtime Issues

1. **Permission denied**: Check file permissions on mounted volumes
2. **Connection refused**: Verify MySQL host and port
3. **Authentication failed**: Check MySQL credentials

## Security Notes

- The container runs as a non-root user (appuser)
- Images are built with minimal dependencies (Alpine Linux)
- No sensitive data is included in the image
- Use secrets management for production credentials 