# Multi-stage build for mysql-cleanup
# Build stage
FROM golang:1.23-alpine AS builder

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

# Build the application for the target architecture
# Buildx will handle multi-arch builds by running this in separate containers
RUN CGO_ENABLED=0 go build -a -installsuffix cgo -o mysql-cleanup ./cmd/mysql-cleanup

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

WORKDIR /app

# Copy the binary
COPY --from=builder /app/mysql-cleanup /app/

# Make the binary executable
RUN chmod +x /app/mysql-cleanup

# Switch to non-root user
USER appuser

# Set the entrypoint
ENTRYPOINT ["./mysql-cleanup"]

# Default command
CMD ["--help"]
