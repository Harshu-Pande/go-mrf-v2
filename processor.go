package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
		header := "provider_reference,negotiated_rate,billing_code,billing_code_type,negotiation_arrangement,negotiated_type,billing_class,billing_code_modifier\n"
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

	// Write a record for each NPI
	for _, npi := range group.GetNPIStrings() {
		// Clean and validate NPI
		npi = strings.TrimSpace(npi)
		if npi == "" {
			continue
		}

		record := fmt.Sprintf("%d,%s,%s,%s\n",
			providerGroupID,
			npi,
			group.TIN.Type,
			group.TIN.Value)

		if _, err := writer.Write([]byte(record)); err != nil {
			return fmt.Errorf("failed to write provider group: %v", err)
		}
	}

	return nil
}

// RemoteReferenceResult represents the result of fetching a remote reference
type RemoteReferenceResult struct {
	Reference *ProviderReference
	Error     error
	Retries   int
}

// RateLimiter provides simple rate limiting functionality
type RateLimiter struct {
	ticker *time.Ticker
	done   chan bool
}

func NewRateLimiter(requestsPerSecond int) *RateLimiter {
	return &RateLimiter{
		ticker: time.NewTicker(time.Second / time.Duration(requestsPerSecond)),
		done:   make(chan bool),
	}
}

func (r *RateLimiter) Wait() {
	select {
	case <-r.ticker.C:
		return
	case <-r.done:
		return
	}
}

func (r *RateLimiter) Stop() {
	r.ticker.Stop()
	close(r.done)
}

// fetchRemoteReference fetches and decodes a remote provider reference with retries
func fetchRemoteReference(client *http.Client, ref RemoteReference, rateLimiter *RateLimiter) (*ProviderReference, error) {
	maxRetries := 3
	backoff := 1 * time.Second

	var lastErr error
	for retry := 0; retry < maxRetries; retry++ {
		if retry > 0 {
			time.Sleep(backoff)
			backoff *= 2 // Exponential backoff
		}

		if rateLimiter != nil {
			rateLimiter.Wait()
		}

		resp, err := client.Get(ref.URL)
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: failed to fetch remote reference: %v", retry+1, err)
			continue
		}

		// Check for rate limiting or server errors that should trigger retry
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("attempt %d: server returned status %d", retry+1, resp.StatusCode)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("remote reference returned status %d", resp.StatusCode)
		}

		var remoteRef ProviderReference
		err = json.NewDecoder(resp.Body).Decode(&remoteRef)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("attempt %d: failed to decode remote reference: %v", retry+1, err)
			continue
		}

		remoteRef.ProviderGroupID = ref.ProviderGroupID
		return &remoteRef, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %v", maxRetries, lastErr)
}

// processRemoteReferences processes remote references in parallel with improved error handling
func processRemoteReferences(refs []RemoteReference) (map[int][]ProviderGroup, error) {
	if len(refs) == 0 {
		return make(map[int][]ProviderGroup), nil
	}

	// Create result map and mutex for concurrent access
	resultMap := make(map[int][]ProviderGroup)
	var resultMutex sync.Mutex

	// Create error tracking
	var errorCount int32
	var firstError atomic.Value

	// Create work channel and result channel
	workChan := make(chan RemoteReference, len(refs))
	resultChan := make(chan RemoteReferenceResult, len(refs))

	// Create rate limiter (100 requests per second)
	rateLimiter := NewRateLimiter(100)
	defer rateLimiter.Stop()

	// Create HTTP client with improved configuration
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			DisableCompression:  false,
			ForceAttemptHTTP2:   true,
		},
	}

	// Start worker goroutines
	var wg sync.WaitGroup
	numWorkers := min(200, len(refs))

	// Create context with timeout for overall processing
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case ref, ok := <-workChan:
					if !ok {
						return
					}
					remoteRef, err := fetchRemoteReference(client, ref, rateLimiter)
					result := RemoteReferenceResult{Reference: remoteRef, Error: err}

					select {
					case resultChan <- result:
					case <-ctx.Done():
						return
					}

					if err != nil {
						if atomic.AddInt32(&errorCount, 1) == 1 {
							firstError.Store(err)
						}
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	// Send work to workers
	go func() {
		for _, ref := range refs {
			select {
			case workChan <- ref:
			case <-ctx.Done():
				return
			}
		}
		close(workChan)
	}()

	// Process results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for result := range resultChan {
		if result.Error != nil {
			continue // Skip failed references but continue processing
		}

		ref := result.Reference
		resultMutex.Lock()
		resultMap[ref.ProviderGroupID] = ref.ProviderGroups
		resultMutex.Unlock()
	}

	// Check context for timeout
	if ctx.Err() != nil {
		return nil, fmt.Errorf("remote reference processing timed out after 10 minutes")
	}

	// If we had errors but got some results, log warning and continue
	if errorCount > 0 {
		firstErr := firstError.Load().(error)
		log.Printf("Warning: %d remote references failed to process. First error: %v", errorCount, firstErr)
	}

	return resultMap, nil
}

// Helper function to convert NPI to string, handling different types
func npiToString(npi interface{}) string {
	switch v := npi.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case json.Number:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Modify streamProviderReferences to handle extraction and CSV conversion
func streamProviderReferences(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) (map[int][]ProviderGroup, error) {
	var providerRefs []ProviderReference
	referenceMap := make(map[int][]ProviderGroup)

	// Create the provider_references directory if it doesn't exist
	refsDir := filepath.Join(outputDir, "provider_references")
	if err := os.MkdirAll(refsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create provider_references directory: %v", err)
	}

	// Create JSON output file for provider references
	jsonOutputPath := filepath.Join(refsDir, "provider_references.json")
	jsonFile, err := os.Create(jsonOutputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create JSON output file: %v", err)
	}
	defer jsonFile.Close()

	// Create JSON encoder for pretty output
	encoder := json.NewEncoder(jsonFile)
	encoder.SetIndent("", "  ")

	// Process provider references array
	for decoder.More() {
		var ref ProviderReference
		if err := decoder.Decode(&ref); err != nil {
			return nil, fmt.Errorf("failed to decode provider reference: %v", err)
		}

		// Store in memory for both JSON and CSV output
		providerRefs = append(providerRefs, ref)

		// Add to reference map
		referenceMap[ref.ProviderGroupID] = ref.ProviderGroups
	}

	// Write all provider references to JSON file
	if err := encoder.Encode(providerRefs); err != nil {
		return nil, fmt.Errorf("failed to write provider references JSON: %v", err)
	}

	// Create CSV file for provider groups
	csvFile, err := os.Create(filepath.Join(refsDir, "provider_groups.csv"))
	if err != nil {
		return nil, fmt.Errorf("failed to create CSV file: %v", err)
	}
	defer csvFile.Close()

	// Create CSV writer
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Write CSV header
	header := []string{"provider_group_id", "npi", "tin_type", "tin_value"}
	if err := writer.Write(header); err != nil {
		return nil, fmt.Errorf("failed to write CSV header: %v", err)
	}

	// Track statistics
	totalNPIs := 0
	uniqueNPIs := make(map[string]bool)
	tinTypes := make(map[string]int)
	totalProviderGroups := 0
	rowsWritten := 0

	// Process each provider reference for CSV output
	for _, ref := range providerRefs {
		for _, group := range ref.ProviderGroups {
			totalProviderGroups++
			tinTypes[group.TIN.Type]++

			for _, npi := range group.NPI {
				npiStr := npiToString(npi)
				if npiStr != "" && npiStr != "null" && npiStr != "<nil>" {
					totalNPIs++
					uniqueNPIs[npiStr] = true

					// Write row to CSV
					row := []string{
						fmt.Sprintf("%d", ref.ProviderGroupID),
						npiStr,
						group.TIN.Type,
						group.TIN.Value,
					}
					if err := writer.Write(row); err != nil {
						return nil, fmt.Errorf("failed to write CSV row: %v", err)
					}
					rowsWritten++
				}
			}
		}
	}

	// Print statistics
	fmt.Printf("\nProvider Reference Processing Stats:\n")
	fmt.Printf("Total provider references: %d\n", len(providerRefs))
	fmt.Printf("Total provider groups: %d\n", totalProviderGroups)
	fmt.Printf("Average groups per reference: %.2f\n", float64(totalProviderGroups)/float64(len(providerRefs)))
	fmt.Printf("\nNPI Stats:\n")
	fmt.Printf("Total NPIs (with duplicates): %d\n", totalNPIs)
	fmt.Printf("Unique NPIs: %d\n", len(uniqueNPIs))
	fmt.Printf("Average NPIs per group: %.2f\n", float64(totalNPIs)/float64(totalProviderGroups))
	fmt.Printf("\nTIN Type Distribution:\n")
	for tinType, count := range tinTypes {
		fmt.Printf("%s: %d (%.2f%%)\n", tinType, count, float64(count)/float64(totalProviderGroups)*100)
	}
	fmt.Printf("\nOutput files:\n")
	fmt.Printf("- JSON: %s\n", jsonOutputPath)
	fmt.Printf("- CSV: %s\n", csvFile.Name())
	fmt.Printf("Total rows written to CSV: %d\n\n", rowsWritten)

	// Consume the array end token
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("failed to read array end token: %v", err)
	}

	return referenceMap, nil
}

func writeNegotiatedRates(item InNetworkItem, rate NegotiatedRate, price NegotiatedPrice, providerRef int, outputDir string, writerPool *FileWriterPool) error {
	// Create filename based on billing code
	filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", item.BillingCode))
	writer, err := writerPool.getOrCreateWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to get writer for billing code %s: %v", item.BillingCode, err)
	}

	modifiers := strings.Join(price.BillingCodeModifier, "|")
	record := fmt.Sprintf("%d,%f,%s,%s,%s,%s,%s,%s\n",
		providerRef,
		price.NegotiatedRate,
		item.BillingCode,
		item.BillingCodeType,
		item.NegotiationArrangement,
		price.NegotiatedType,
		price.BillingClass,
		modifiers)

	if _, err := writer.Write([]byte(record)); err != nil {
		return fmt.Errorf("failed to write negotiated rate: %v", err)
	}

	return nil
}

// Worker function to process in-network items
func processInNetworkItem(item InNetworkItem, outputDir string, writerPool *FileWriterPool, itemCount *int64) error {
	count := atomic.AddInt64(itemCount, 1)

	// Print each item being processed
	fmt.Printf("Processing in-network item %d: %s\n", count, item.BillingCode)
	os.Stdout.Sync() // Ensure output is flushed immediately

	// Create filename based on billing code
	filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", item.BillingCode))

	// Pre-allocate buffer for all records
	var builder strings.Builder
	builder.Grow(1024 * 8) // Pre-allocate 8KB

	// Process each negotiated rate
	for _, rate := range item.NegotiatedRates {
		for _, price := range rate.NegotiatedPrices {
			// Process each provider reference
			for _, providerRef := range rate.ProviderReferences {
				// Convert billing_code_modifier array to string
				modifiers := strings.Join(price.BillingCodeModifier, "|")

				// Build the CSV record
				record := fmt.Sprintf("%d,%f,%s,%s,%s,%s,%s,%s\n",
					providerRef,
					price.NegotiatedRate,
					item.BillingCode,
					item.BillingCodeType,
					item.NegotiationArrangement,
					price.NegotiatedType,
					price.BillingClass,
					modifiers)

				builder.WriteString(record)
			}
		}
	}

	// Write all records at once
	if builder.Len() > 0 {
		return writerPool.WriteAsync(filename, []byte(builder.String()))
	}

	return nil
}

func streamInNetworkItems(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool, referenceMap map[int][]ProviderGroup) error {
	// Create buffered channels for processing
	itemChan := make(chan InNetworkItem, processorConfig.NumWorkers*2)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	var itemCount int64

	// Start worker goroutines
	fmt.Printf("Starting %d workers for parallel processing...\n", processorConfig.NumWorkers)
	os.Stdout.Sync()

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

// copyNextObject copies a complete JSON object from the decoder to the writer
func copyNextObject(decoder *json.Decoder, writer io.Writer) error {
	// Read the first token which should be '{' for an object
	t, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object start, got %v", t)
	}

	// Write the opening brace
	if _, err := writer.Write([]byte("{")); err != nil {
		return err
	}

	depth := 1
	first := true

	// Copy tokens until we complete the object
	for depth > 0 {
		t, err := decoder.Token()
		if err != nil {
			return err
		}

		if !first {
			if _, err := writer.Write([]byte(",")); err != nil {
				return err
			}
		}
		first = false

		switch v := t.(type) {
		case json.Delim:
			if v == '{' || v == '[' {
				depth++
			} else if v == '}' || v == ']' {
				depth--
			}
			if _, err := writer.Write([]byte(string(v))); err != nil {
				return err
			}
		case string:
			if _, err := fmt.Fprintf(writer, "%q", v); err != nil {
				return err
			}
		case float64:
			if _, err := fmt.Fprintf(writer, "%g", v); err != nil {
				return err
			}
		case bool:
			if _, err := fmt.Fprintf(writer, "%t", v); err != nil {
				return err
			}
		case nil:
			if _, err := writer.Write([]byte("null")); err != nil {
				return err
			}
		}
	}

	return nil
}

// ProcessFile processes a gzipped MRF JSON file
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

	// First pass: Extract and process provider_references
	fmt.Printf("First pass: Extracting provider references from %s\n", inputFile)
	referenceMap, err := extractProviderReferences(inputFile, outputDir)
	if err != nil {
		return fmt.Errorf("failed to extract provider references: %v", err)
	}

	// Second pass: Stream and process in_network items
	fmt.Printf("Second pass: Processing in_network items\n")
	if err := processInNetworkItems(inputFile, outputDir, writerPool, referenceMap); err != nil {
		return fmt.Errorf("failed to process in-network items: %v", err)
	}

	return nil
}

// extractProviderReferences reads just the provider_references section from the file
func extractProviderReferences(inputFile string, outputDir string) (map[int][]ProviderGroup, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	// Create a decoder that will decode numbers as json.Number to preserve precision
	decoder := json.NewDecoder(gzReader)
	decoder.UseNumber()

	// Create provider_references directory
	refsDir := filepath.Join(outputDir, "provider_references")
	if err := os.MkdirAll(refsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create provider_references directory: %v", err)
	}

	// Read opening brace of the root object
	t, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("failed to read opening brace: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected opening brace, got %v", t)
	}

	// Skip metadata fields until we find provider_references
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("failed to read token: %v", err)
		}

		if str, ok := t.(string); ok {
			if str == "provider_references" {
				// Found provider_references field, decode it directly
				var providerRefs []ProviderReference
				if err := decoder.Decode(&providerRefs); err != nil {
					return nil, fmt.Errorf("failed to decode provider references: %v", err)
				}

				// Write to JSON file
				jsonFile, err := os.Create(filepath.Join(refsDir, "provider_references.json"))
				if err != nil {
					return nil, fmt.Errorf("failed to create JSON file: %v", err)
				}
				encoder := json.NewEncoder(jsonFile)
				encoder.SetIndent("", "  ")
				if err := encoder.Encode(providerRefs); err != nil {
					jsonFile.Close()
					return nil, fmt.Errorf("failed to write JSON: %v", err)
				}
				jsonFile.Close()

				// Create CSV file
				csvFile, err := os.Create(filepath.Join(refsDir, "provider_groups.csv"))
				if err != nil {
					return nil, fmt.Errorf("failed to create CSV file: %v", err)
				}
				writer := csv.NewWriter(csvFile)

				// Write CSV header
				if err := writer.Write([]string{"provider_group_id", "npi", "tin_type", "tin_value"}); err != nil {
					csvFile.Close()
					return nil, fmt.Errorf("failed to write CSV header: %v", err)
				}

				// Build reference map and write CSV records
				referenceMap := make(map[int][]ProviderGroup)
				for _, ref := range providerRefs {
					referenceMap[ref.ProviderGroupID] = ref.ProviderGroups
					for _, group := range ref.ProviderGroups {
						for _, npi := range group.GetNPIStrings() {
							if npi != "" && npi != "null" && npi != "<nil>" {
								record := []string{
									fmt.Sprintf("%d", ref.ProviderGroupID),
									npi,
									group.TIN.Type,
									group.TIN.Value,
								}
								if err := writer.Write(record); err != nil {
									csvFile.Close()
									return nil, fmt.Errorf("failed to write CSV record: %v", err)
								}
							}
						}
					}
				}

				writer.Flush()
				csvFile.Close()

				fmt.Printf("Extracted %d provider references\n", len(providerRefs))
				return referenceMap, nil
			} else {
				// Skip other top-level fields
				if err := decoder.Decode(new(interface{})); err != nil {
					return nil, fmt.Errorf("failed to skip field %s: %v", str, err)
				}
			}
		}
	}

	return nil, fmt.Errorf("provider_references section not found in file")
}

// processInNetworkItems streams and processes the in_network section
func processInNetworkItems(inputFile string, outputDir string, writerPool *FileWriterPool, referenceMap map[int][]ProviderGroup) error {
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

	decoder := json.NewDecoder(gzReader)
	decoder.UseNumber()

	// Read opening brace of the root object
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening brace: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected opening brace, got %v", t)
	}

	// Skip fields until we find in_network
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}

		if str, ok := t.(string); ok {
			if str == "in_network" {
				// Found in_network field, expect array start
				t, err := decoder.Token()
				if err != nil {
					return fmt.Errorf("failed to read in_network array start: %v", err)
				}
				if delim, ok := t.(json.Delim); !ok || delim != '[' {
					return fmt.Errorf("expected array start, got %v", t)
				}

				// Create in_network directory
				inNetworkDir := filepath.Join(outputDir, "in_network")
				if err := os.MkdirAll(inNetworkDir, 0755); err != nil {
					return fmt.Errorf("failed to create in_network directory: %v", err)
				}

				// Stream and process in_network items
				return streamInNetworkItems(decoder, outputDir, writerPool, referenceMap)
			} else {
				// Skip other top-level fields
				if err := decoder.Decode(new(interface{})); err != nil {
					return fmt.Errorf("failed to skip field %s: %v", str, err)
				}
			}
		}
	}

	return fmt.Errorf("in_network section not found in file")
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

	// Close all writers first
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

		// Get output directory from first writer
		var outputDir string
		for _, writer := range pool.writers {
			outputDir = filepath.Dir(filepath.Dir(writer.file.Name()))
			break
		}
		if outputDir == "" {
			return fmt.Errorf("no output directory found")
		}

		// Walk through the output directory to find all files
		var filesToUpload []string
		err := filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() && strings.HasSuffix(path, ".csv") {
				filesToUpload = append(filesToUpload, path)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("failed to walk output directory: %v", err)
		}

		fmt.Printf("Found %d files to upload\n", len(filesToUpload))

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

// Add this helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
