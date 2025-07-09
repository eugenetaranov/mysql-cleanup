# ---- Build Stage ----
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o mysql_cleanup main.go

# ---- Run Stage ----
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/mysql_cleanup ./mysql_cleanup

# Set up non-root user (optional, for security)
RUN adduser -D appuser
USER appuser

ENTRYPOINT ["./mysql_cleanup"]
