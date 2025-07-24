#!/bin/bash

# Multi-architecture build script for mysql_cleanup
# This script builds Docker images for multiple architectures and pushes to Docker Hub

set -e

# Configuration
IMAGE_NAME="your-dockerhub-username/mysql-cleanup"  # Replace with your Docker Hub username
VERSION=${1:-latest}

echo "Building multi-architecture Docker image: $IMAGE_NAME:$VERSION"

# Create and use a new builder instance that supports multi-architecture builds
docker buildx create --name multiarch-builder --use || true

# Build and push for multiple architectures
docker buildx build \
    --platform linux/amd64,linux/arm64 \
    --tag "$IMAGE_NAME:$VERSION" \
    --tag "$IMAGE_NAME:latest" \
    --push \
    .

echo "Successfully built and pushed $IMAGE_NAME:$VERSION for multiple architectures"
echo "Architectures: linux/amd64, linux/arm64"

# Optional: Also build for macOS (darwin) if needed
# Note: Docker Hub doesn't support macOS images, but you can build them locally
echo ""
echo "Building for local macOS use..."
docker buildx build \
    --platform darwin/amd64,darwin/arm64 \
    --tag "$IMAGE_NAME:macos-$VERSION" \
    --load \
    .

echo "Successfully built macOS versions locally"
echo "You can run: docker run --rm $IMAGE_NAME:macos-$VERSION --help" 