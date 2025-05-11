package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type R2Config struct {
	AccountID       string
	AccessKeyID     string
	AccessKeySecret string
	BucketName      string
	client          *s3.Client
}

func NewR2Config() *R2Config {
	return &R2Config{
		AccountID:       os.Getenv("R2_ACCOUNT_ID"),
		AccessKeyID:     os.Getenv("R2_ACCESS_KEY_ID"),
		AccessKeySecret: os.Getenv("R2_ACCESS_KEY_SECRET"),
		BucketName:      os.Getenv("R2_BUCKET_NAME"),
	}
}

func (r *R2Config) Initialize() error {
	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", r.AccountID),
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			r.AccessKeyID,
			r.AccessKeySecret,
			"",
		)),
		config.WithRegion("auto"),
	)
	if err != nil {
		return fmt.Errorf("unable to load SDK config: %v", err)
	}

	r.client = s3.NewFromConfig(cfg)
	return nil
}

func (r *R2Config) UploadFile(localPath, remotePrefix string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("unable to open file %s: %v", localPath, err)
	}
	defer file.Close()

	// Create the remote path by combining prefix and filename
	remotePath := filepath.Join(remotePrefix, filepath.Base(localPath))
	// Convert Windows path separators to forward slashes if needed
	remotePath = strings.ReplaceAll(remotePath, "\\", "/")

	_, err = r.client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(r.BucketName),
		Key:    aws.String(remotePath),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("unable to upload file to R2: %v", err)
	}

	return nil
}
