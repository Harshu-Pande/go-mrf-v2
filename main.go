package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/unix"
)

func printUsage() {
	fmt.Printf(`MRF JSON Processor - Process healthcare price transparency files

Usage: %s [options] <input_file.json.gz> <output_dir>

Options:
  -workers int
        Number of worker threads (default: number of CPU cores)
  -buffer int
        Write buffer size in MB (default: 1)
  -r2-upload
        Upload output files to Cloudflare R2 (requires R2 environment variables)
  -r2-prefix string
        Prefix for files in R2 bucket (default: "mrf-output")
  -keep-local
        Keep local files after uploading to R2 (default: true)

Environment Variables for R2:
  R2_ACCOUNT_ID      Your Cloudflare account ID
  R2_ACCESS_KEY_ID   R2 access key ID
  R2_ACCESS_KEY_SECRET R2 access key secret
  R2_BUCKET_NAME     R2 bucket name

Example:
  %s input.json.gz output/
`, os.Args[0], os.Args[0])
}

func main() {
	// Parse command line flags
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Number of worker threads")
	bufferSize := flag.Int("buffer", 1, "Write buffer size in MB")
	useR2 := flag.Bool("r2-upload", false, "Upload output files to Cloudflare R2")
	r2Prefix := flag.String("r2-prefix", "mrf-output", "Prefix for files in R2 bucket")
	keepLocal := flag.Bool("keep-local", true, "Keep local files after uploading to R2")
	flag.Usage = printUsage
	flag.Parse()

	// Validate command line arguments
	args := flag.Args()
	if len(args) != 2 {
		printUsage()
		os.Exit(1)
	}

	// Validate R2 flags
	if !*useR2 && !*keepLocal {
		fmt.Println("Error: Cannot disable both local storage and R2 upload. At least one must be enabled.")
		os.Exit(1)
	}

	inputFile := args[0]
	outputDir := args[1]

	// Validate input file
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		fmt.Printf("Error: Input file '%s' does not exist\n", inputFile)
		os.Exit(1)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error: Failed to create output directory '%s': %v\n", outputDir, err)
		os.Exit(1)
	}

	// Set processor configuration
	SetConfig(ProcessorConfig{
		WriteBufferSize: *bufferSize * 1024 * 1024,
		NumWorkers:      *numWorkers,
		KeepLocal:       *keepLocal,
	})

	// Initialize R2 if requested
	var r2Config *R2Config
	if *useR2 {
		r2Config = NewR2Config()
		if err := r2Config.Initialize(); err != nil {
			fmt.Printf("Error: Failed to initialize R2: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("R2 configuration initialized. Files will be uploaded to bucket: %s\n", r2Config.BucketName)
	}

	// Set GOMAXPROCS to use specified number of workers
	runtime.GOMAXPROCS(*numWorkers)

	// Increase resource limits
	var rLimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rLimit); err == nil {
		rLimit.Cur = rLimit.Max
		unix.Setrlimit(unix.RLIMIT_NOFILE, &rLimit)
	}

	// Process the file
	fmt.Printf("Processing %s with %d workers...\n", filepath.Base(inputFile), *numWorkers)
	if err := ProcessFile(inputFile, outputDir, r2Config, *r2Prefix); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Processing complete. Output written to %s\n", outputDir)
	if *useR2 {
		fmt.Printf("Files have been uploaded to R2 bucket: %s with prefix: %s\n", r2Config.BucketName, *r2Prefix)
	}
}
