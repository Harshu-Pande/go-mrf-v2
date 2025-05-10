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

Example:
  %s input.json.gz output/
`, os.Args[0], os.Args[0])
}

func main() {
	// Parse command line flags
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Number of worker threads")
	bufferSize := flag.Int("buffer", 1, "Write buffer size in MB")
	flag.Usage = printUsage
	flag.Parse()

	// Validate command line arguments
	args := flag.Args()
	if len(args) != 2 {
		printUsage()
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
	})

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
	if err := ProcessFile(inputFile, outputDir); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Processing complete. Output written to %s\n", outputDir)
}
