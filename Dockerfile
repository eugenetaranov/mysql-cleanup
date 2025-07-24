# Multi-stage build for mysql_cleanup
# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application for multiple architectures
# We'll use buildx to handle multi-arch builds
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -installsuffix cgo -o mysql_cleanup-linux-amd64 .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -installsuffix cgo -o mysql_cleanup-linux-arm64 .
RUN CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -a -installsuffix cgo -o mysql_cleanup-darwin-amd64 .
RUN CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -a -installsuffix cgo -o mysql_cleanup-darwin-arm64 .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

# Copy the appropriate binary based on target architecture
# This will be handled by buildx during the build process
COPY --from=builder /app/mysql_cleanup-linux-* /app/

# Make the binary executable
RUN chmod +x /app/mysql_cleanup-*

# Switch to non-root user
USER appuser

# Set the entrypoint
ENTRYPOINT ["./mysql_cleanup-linux-amd64"]

# Default command
CMD ["--help"]
