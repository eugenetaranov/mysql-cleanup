package cleanup

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3HandlerImpl handles S3 file operations
type S3HandlerImpl struct {
	logger Logger
}

// NewS3Handler creates a new S3Handler
func NewS3Handler(logger Logger) S3Handler {
	return &S3HandlerImpl{
		logger: logger,
	}
}

// DownloadS3File downloads a file from S3 to a local temp file
// Returns the local file path and any error
func (s *S3HandlerImpl) DownloadS3File(s3URI string) (string, error) {
	if !strings.HasPrefix(s3URI, "s3://") {
		return "", fmt.Errorf("not an S3 URI: %s", s3URI)
	}

	// Parse S3 URI: s3://bucket/key
	uri := strings.TrimPrefix(s3URI, "s3://")
	parts := strings.SplitN(uri, "/", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid S3 URI format: %s", s3URI)
	}
	bucket := parts[0]
	key := parts[1]

	s.logger.Info("Downloading file from S3",
		String("s3_uri", s3URI),
		String("bucket", bucket),
		String("key", key))

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		s.logger.Error("Failed to load AWS config", Error("error", err))
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Create downloader
	downloader := manager.NewDownloader(s3Client)

	// Create temp file
	tempFile, err := os.CreateTemp("", "s3-download-*")
	if err != nil {
		s.logger.Error("Failed to create temp file", Error("error", err))
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Download file
	_, err = downloader.Download(context.TODO(), tempFile, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		s.logger.Error("Failed to download file from S3",
			Error("error", err),
			String("s3_uri", s3URI))
		os.Remove(tempFile.Name()) // Clean up temp file
		return "", fmt.Errorf("failed to download file from S3: %w", err)
	}

	s.logger.Info("Successfully downloaded file from S3",
		String("s3_uri", s3URI),
		String("local_path", tempFile.Name()))

	return tempFile.Name(), nil
}

// CleanupTempFile removes a temporary file
func (s *S3HandlerImpl) CleanupTempFile(filePath string) error {
	if filePath == "" {
		return nil
	}

	err := os.Remove(filePath)
	if err != nil {
		s.logger.Error("Failed to cleanup temp file",
			Error("error", err),
			String("file_path", filePath))
		return fmt.Errorf("failed to cleanup temp file: %w", err)
	}

	s.logger.Debug("Cleaned up temp file", String("file_path", filePath))
	return nil
}
