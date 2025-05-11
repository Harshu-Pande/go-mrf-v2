package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ProcessorConfig holds configuration for the processor
type ProcessorConfig struct {
	WriteBufferSize int
	NumWorkers      int
	KeepLocal       bool
}

// DefaultConfig returns the default processor configuration
func DefaultConfig() ProcessorConfig {
	return ProcessorConfig{
		WriteBufferSize: 1024 * 1024, // 1MB write buffer
		NumWorkers:      runtime.NumCPU(),
		KeepLocal:       true,
	}
}

var processorConfig ProcessorConfig

// SetConfig sets the processor configuration
func SetConfig(cfg ProcessorConfig) {
	processorConfig = cfg
}

// Constants for optimization
const (
	maxOpenFiles = 100
	batchSize    = 1000
)

// Chunk represents a portion of the file to process
type Chunk struct {
	Data  []byte
	Start int64
	End   int64
}

// Result represents processed data
type Result struct {
	ProviderGroups     []ProviderGroup
	ProviderReferences []ProviderReference
	NegotiatedRates    []NegotiatedRate
}

// FileWriter handles buffered writing to files
type FileWriter struct {
	file         *os.File
	writer       *bufio.Writer
	mu           sync.Mutex
	written      int64
	lastUse      time.Time
	buffer       []byte
	bufferSize   int
	r2Config     *R2Config
	remotePrefix string
}

// Pool of FileWriters with LRU cache
type FileWriterPool struct {
	writers      map[string]*FileWriter
	mu           sync.RWMutex
	maxWriters   int
	bufferSize   int
	writeBuffer  chan writeRequest
	r2Config     *R2Config
	remotePrefix string
}

type writeRequest struct {
	filename string
	data     []byte
	errChan  chan error
}

// NewFileWriterPool creates a new pool with a maximum number of writers
func NewFileWriterPool(maxWriters int) *FileWriterPool {
	pool := &FileWriterPool{
		writers:     make(map[string]*FileWriter),
		maxWriters:  maxWriters,
		bufferSize:  8 * 1024 * 1024, // 8MB buffer for better performance
		writeBuffer: make(chan writeRequest, maxWriters*2),
	}

	// Start background writer
	go pool.backgroundWriter()
	return pool
}

func (pool *FileWriterPool) backgroundWriter() {
	for req := range pool.writeBuffer {
		writer, err := pool.getOrCreateWriter(req.filename)
		if err != nil {
			req.errChan <- err
			continue
		}

		_, err = writer.Write(req.data)
		req.errChan <- err
	}
}

func (pool *FileWriterPool) getOrCreateWriter(filename string) (*FileWriter, error) {
	// Try read lock first
	pool.mu.RLock()
	writer, exists := pool.writers[filename]
	pool.mu.RUnlock()
	if exists {
		return writer, nil
	}

	// Need write lock
	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Check again in case another goroutine created it
	if writer, exists = pool.writers[filename]; exists {
		return writer, nil
	}

	// If we've reached max writers, close least recently used
	if len(pool.writers) >= pool.maxWriters {
		if err := pool.closeLeastRecentlyUsed(); err != nil {
			return nil, err
		}
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %v", dir, err)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
	}

	writer = &FileWriter{
		file:         file,
		writer:       bufio.NewWriterSize(file, pool.bufferSize),
		lastUse:      time.Now(),
		bufferSize:   pool.bufferSize,
		r2Config:     pool.r2Config,
		remotePrefix: pool.remotePrefix,
	}

	// Write header if file is new (size is 0)
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %v", err)
	}

	if info.Size() == 0 && strings.Contains(filename, "in_network_") {
		header := "provider_reference,negotiated_rate,billing_code,billing_code_type,negotiation_arrangement,negotiated_type,billing_class\n"
		if _, err := writer.Write([]byte(header)); err != nil {
			return nil, fmt.Errorf("failed to write header: %v", err)
		}
	}

	pool.writers[filename] = writer
	return writer, nil
}

func (pool *FileWriterPool) WriteAsync(filename string, data []byte) error {
	errChan := make(chan error, 1)
	pool.writeBuffer <- writeRequest{
		filename: filename,
		data:     data,
		errChan:  errChan,
	}
	return <-errChan
}

func (fw *FileWriter) Write(data []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.lastUse = time.Now()

	// If buffer is nil or too small, create new one
	if fw.buffer == nil || cap(fw.buffer) < len(data) {
		fw.buffer = make([]byte, 0, max(len(data), 1024*1024))
	}

	// Reset buffer and copy data
	fw.buffer = fw.buffer[:0]
	fw.buffer = append(fw.buffer, data...)

	// Write in a single operation
	n, err := fw.writer.Write(fw.buffer)
	if err != nil {
		return n, fmt.Errorf("write error: %v", err)
	}
	fw.written += int64(n)
	return n, nil
}

func (fw *FileWriter) Flush() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.writer.Flush()
}

func (fw *FileWriter) Close() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	if err := fw.writer.Flush(); err != nil {
		return err
	}

	// Close the file
	if err := fw.file.Close(); err != nil {
		return err
	}

	return nil
}

func (pool *FileWriterPool) closeLeastRecentlyUsed() error {
	var lru *FileWriter
	var lruKey string
	var lruTime = time.Now()

	// Find least recently used writer
	for k, w := range pool.writers {
		if w.lastUse.Before(lruTime) {
			lru = w
			lruKey = k
			lruTime = w.lastUse
		}
	}

	if lru != nil {
		if err := lru.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
		delete(pool.writers, lruKey)
	}
	return nil
}

func writeProviderGroups(group ProviderGroup, providerGroupID int, outputDir string, writerPool *FileWriterPool) error {
	filename := filepath.Join(outputDir, "provider_groups.csv")
	writer, err := writerPool.getOrCreateWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to get writer for provider groups: %v", err)
	}

	// Handle different NPI types
	var npiStr string
	switch npi := group.NPI.(type) {
	case string:
		npiStr = npi
	case float64:
		npiStr = fmt.Sprintf("%d", int(npi))
	case []interface{}:
		// Take the first NPI if it's an array
		if len(npi) > 0 {
			if str, ok := npi[0].(string); ok {
				npiStr = str
			} else if num, ok := npi[0].(float64); ok {
				npiStr = fmt.Sprintf("%d", int(num))
			}
		}
	}

	record := fmt.Sprintf("%d,%s,%s,%s\n",
		providerGroupID,
		npiStr,
		group.TIN.Type,
		group.TIN.Value)

	if _, err := writer.Write([]byte(record)); err != nil {
		return fmt.Errorf("failed to write provider group: %v", err)
	}

	return nil
}

func streamProviderReferences(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) error {
	// Process provider references array
	for decoder.More() {
		var ref ProviderReference
		if err := decoder.Decode(&ref); err != nil {
			return fmt.Errorf("failed to decode provider reference: %v", err)
		}

		for _, group := range ref.ProviderGroups {
			if err := writeProviderGroups(group, ref.ProviderGroupID, outputDir, writerPool); err != nil {
				return err
			}
		}
	}

	// Consume the array end token
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("failed to read array end token: %v", err)
	}
	return nil
}

func writeNegotiatedRates(item InNetworkItem, rate NegotiatedRate, price NegotiatedPrice, providerRef int, outputDir string, writerPool *FileWriterPool) error {
	// Create filename based on billing code
	filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", item.BillingCode))
	writer, err := writerPool.getOrCreateWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to get writer for billing code %s: %v", item.BillingCode, err)
	}

	record := fmt.Sprintf("%d,%f,%s,%s,%s,%s,%s\n",
		providerRef,
		price.NegotiatedRate,
		item.BillingCode,
		item.BillingCodeType,
		item.NegotiationArrangement,
		price.NegotiatedType,
		price.BillingClass)

	if _, err := writer.Write([]byte(record)); err != nil {
		return fmt.Errorf("failed to write negotiated rate: %v", err)
	}

	return nil
}

// Worker function to process in-network items
func processInNetworkItem(item InNetworkItem, outputDir string, writerPool *FileWriterPool, itemCount *int64) error {
	count := atomic.AddInt64(itemCount, 1)

	// Always print the item being processed
	fmt.Printf("Processing in-network item %d: %s\n", count, item.BillingCode)

	// Pre-allocate buffer for all records
	var builder strings.Builder
	builder.Grow(1024 * 8) // Pre-allocate 8KB

	// Build all records in memory first
	for _, rate := range item.NegotiatedRates {
		for _, price := range rate.NegotiatedPrices {
			for _, providerRef := range rate.ProviderReferences {
				fmt.Fprintf(&builder, "%d,%f,%s,%s,%s,%s,%s\n",
					providerRef,
					price.NegotiatedRate,
					item.BillingCode,
					item.BillingCodeType,
					item.NegotiationArrangement,
					price.NegotiatedType,
					price.BillingClass)
			}
		}
	}

	// Write all records at once if we have any
	if builder.Len() > 0 {
		filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", item.BillingCode))
		return writerPool.WriteAsync(filename, []byte(builder.String()))
	}

	return nil
}

func streamInNetworkItems(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) error {
	// Create a buffered channel for items
	itemChan := make(chan InNetworkItem, processorConfig.NumWorkers*2)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	var itemCount int64

	// Start worker goroutines
	fmt.Printf("Starting %d workers for parallel processing...\n", processorConfig.NumWorkers)
	os.Stdout.Sync() // Force flush stdout

	for i := 0; i < processorConfig.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range itemChan {
				if err := processInNetworkItem(item, outputDir, writerPool, &itemCount); err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Process items and track performance
	startTime := time.Now()
	lastUpdateTime := startTime

	go func() {
		defer close(itemChan)
		for decoder.More() {
			var item InNetworkItem
			if err := decoder.Decode(&item); err != nil {
				select {
				case errChan <- fmt.Errorf("failed to decode in-network item: %v", err):
				default:
				}
				return
			}
			itemChan <- item

			// Update performance stats every 5 seconds
			now := time.Now()
			if now.Sub(lastUpdateTime) >= 5*time.Second {
				current := atomic.LoadInt64(&itemCount)
				elapsed := now.Sub(startTime).Seconds()
				itemsPerSec := float64(current) / elapsed
				fmt.Printf("\n--- Performance: %.1f items/sec (processed %d items in %.1f seconds) ---\n\n",
					itemsPerSec, current, elapsed)
				os.Stdout.Sync()
				lastUpdateTime = now
			}
		}
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Final statistics
	totalTime := time.Since(startTime)
	totalCount := atomic.LoadInt64(&itemCount)
	itemsPerSec := float64(totalCount) / totalTime.Seconds()

	fmt.Printf("\nCompleted processing %d items in %.1f seconds (%.1f items/sec)\n",
		totalCount, totalTime.Seconds(), itemsPerSec)
	os.Stdout.Sync()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	return nil
}

func ProcessFile(inputFile, outputDir string, r2Config *R2Config, remotePrefix string) error {
	fmt.Printf("Starting processing with %d CPU cores...\n", runtime.NumCPU())

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Initialize the writer pool with R2 config
	writerPool := NewFileWriterPool(runtime.NumCPU())
	writerPool.r2Config = r2Config
	writerPool.remotePrefix = remotePrefix
	defer writerPool.Close()

	// Create CSV files with headers
	headers := map[string]string{
		"provider_groups.csv":  "provider_group_id,npi,tin_type,tin_value\n",
		"negotiated_rates.csv": "provider_reference,negotiated_rate,billing_code,billing_code_type,negotiation_arrangement,negotiated_type,billing_class\n",
	}

	for filename, header := range headers {
		if err := writerPool.WriteAsync(filepath.Join(outputDir, filename), []byte(header)); err != nil {
			return fmt.Errorf("failed to write header to %s: %v", filename, err)
		}
	}

	// Open and decompress the file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	// Create a JSON decoder that reads from the gzip reader
	decoder := json.NewDecoder(gzReader)

	// Read opening brace
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening brace: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected opening brace, got %v", t)
	}

	// Process the JSON stream
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}

		// Process each top-level field
		key, ok := t.(string)
		if !ok {
			continue
		}

		switch key {
		case "provider_references":
			// Start of provider_references array
			t, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read provider_references array start: %v", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return fmt.Errorf("expected array start, got %v", t)
			}
			if err := streamProviderReferences(decoder, outputDir, writerPool); err != nil {
				return fmt.Errorf("failed to process provider references: %v", err)
			}

		case "in_network":
			// Start of in_network array
			t, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read in_network array start: %v", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return fmt.Errorf("expected array start, got %v", t)
			}
			if err := streamInNetworkItems(decoder, outputDir, writerPool); err != nil {
				return fmt.Errorf("failed to process in-network items: %v", err)
			}

		default:
			// Skip other fields
			if _, err := decoder.Token(); err != nil {
				return fmt.Errorf("failed to skip value: %v", err)
			}
		}
	}

	return nil
}

// Helper function for max
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (pool *FileWriterPool) Close() error {
	close(pool.writeBuffer)

	pool.mu.Lock()
	defer pool.mu.Unlock()

	var lastErr error
	for _, writer := range pool.writers {
		if err := writer.Close(); err != nil {
			lastErr = err
		}
	}

	// If R2 upload is enabled, upload all files after processing
	if pool.r2Config != nil {
		fmt.Printf("\nUploading files to R2 bucket %s...\n", pool.r2Config.BucketName)

		// Create a channel to limit concurrent uploads
		sem := make(chan struct{}, 10) // Allow 10 concurrent uploads
		var wg sync.WaitGroup
		var uploadErr error
		var uploadErrMu sync.Mutex

		// Get a list of all files to upload
		var filesToUpload []string
		for path := range pool.writers {
			filesToUpload = append(filesToUpload, path)
		}

		// Upload each file concurrently
		for _, filePath := range filesToUpload {
			wg.Add(1)
			go func(path string) {
				defer wg.Done()
				sem <- struct{}{}        // Acquire semaphore
				defer func() { <-sem }() // Release semaphore

				// Upload the file
				if err := pool.r2Config.UploadFile(path, pool.remotePrefix); err != nil {
					uploadErrMu.Lock()
					uploadErr = err
					uploadErrMu.Unlock()
					fmt.Printf("Error uploading %s: %v\n", path, err)
					return
				}

				// If we're not keeping local files, delete after successful upload
				if !processorConfig.KeepLocal {
					if err := os.Remove(path); err != nil {
						fmt.Printf("Warning: Failed to remove local file %s after R2 upload: %v\n", path, err)
					}
				}

				fmt.Printf("Uploaded %s\n", path)
			}(filePath)
		}

		// Wait for all uploads to complete
		wg.Wait()

		if uploadErr != nil {
			return fmt.Errorf("some files failed to upload: %v", uploadErr)
		}

		fmt.Printf("All files uploaded to R2 bucket: %s with prefix: %s\n", pool.r2Config.BucketName, pool.remotePrefix)
	}

	return lastErr
}
