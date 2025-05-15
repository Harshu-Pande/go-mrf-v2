package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
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

	// Get TIN values with safe defaults
	tinType := "unknown"
	if group.TIN.Type != nil {
		tinType = *group.TIN.Type
	}

	tinValue := "unknown"
	if group.TIN.Value != nil {
		tinValue = *group.TIN.Value
	}

	record := fmt.Sprintf("%d,%s,%s,%s\n",
		providerGroupID,
		npiStr,
		tinType,
		tinValue)

	if _, err := writer.Write([]byte(record)); err != nil {
		return fmt.Errorf("failed to write provider group: %v", err)
	}

	return nil
}

func streamProviderReferences(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) error {
	filename := filepath.Join(outputDir, "provider_groups.csv")
	writer, err := writerPool.getOrCreateWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to create provider groups file: %v", err)
	}

	// Write header if file is empty
	info, err := writer.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %v", err)
	}

	if info.Size() == 0 {
		header := "provider_group_id,npi,tin_type,tin_value\n"
		if _, err := writer.Write([]byte(header)); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Read opening bracket
	t, err := decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil // Empty array is valid
		}
		return fmt.Errorf("failed to read array start: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected array start, got %v", t)
	}

	fmt.Printf("Processing provider references...\n")
	var count int64
	var skippedNPIs int64
	var malformedTINs int64
	var skippedRefs int64

	// Process provider references array
	for decoder.More() {
		var ref ProviderReference
		if err := decoder.Decode(&ref); err != nil {
			fmt.Printf("WARNING: Failed to decode provider reference: %v\n", err)
			skippedRefs++
			// Try to skip the malformed object by reading until next valid token
			depth := 0
			for {
				t, err := decoder.Token()
				if err != nil {
					if err == io.EOF {
						return fmt.Errorf("unexpected EOF while skipping malformed object")
					}
					return fmt.Errorf("error while skipping malformed object: %v", err)
				}
				if delim, ok := t.(json.Delim); ok {
					switch delim {
					case '{', '[':
						depth++
					case '}', ']':
						depth--
						if depth < 0 {
							break
						}
					}
				}
				if depth <= 0 {
					break
				}
			}
			continue
		}

		// Use default provider group ID if missing
		if ref.ProviderGroupID == nil {
			defaultID := 0
			ref.ProviderGroupID = &defaultID
			fmt.Printf("INFO: Using default provider group ID (0) for reference\n")
		}

		if len(ref.ProviderGroups) == 0 {
			// Create a default provider group
			defaultGroup := ProviderGroup{
				NPI: "",
				TIN: struct {
					Type  *string `json:"type,omitempty"`
					Value *string `json:"value,omitempty"`
				}{
					Type:  new(string),
					Value: new(string),
				},
			}
			ref.ProviderGroups = []ProviderGroup{defaultGroup}
			fmt.Printf("INFO: Created default provider group for ID %d\n", *ref.ProviderGroupID)
		}

		for _, group := range ref.ProviderGroups {
			// Handle NPI with more formats
			var npiStr string
			switch npi := group.NPI.(type) {
			case string:
				npiStr = npi
			case float64:
				npiStr = fmt.Sprintf("%d", int(npi))
			case int:
				npiStr = fmt.Sprintf("%d", npi)
			case []interface{}:
				if len(npi) > 0 {
					switch v := npi[0].(type) {
					case string:
						npiStr = v
					case float64:
						npiStr = fmt.Sprintf("%d", int(v))
					case int:
						npiStr = fmt.Sprintf("%d", v)
					default:
						npiStr = fmt.Sprintf("%v", v)
					}
				}
			case nil:
				npiStr = ""
			default:
				npiStr = fmt.Sprintf("%v", npi)
			}

			// Initialize TIN fields if nil
			if group.TIN.Type == nil {
				empty := ""
				group.TIN.Type = &empty
			}
			if group.TIN.Value == nil {
				empty := ""
				group.TIN.Value = &empty
			}

			record := fmt.Sprintf("%d,%s,%s,%s\n",
				*ref.ProviderGroupID,
				npiStr,
				*group.TIN.Type,
				*group.TIN.Value)

			if _, err := writer.Write([]byte(record)); err != nil {
				return fmt.Errorf("failed to write provider group: %v", err)
			}
		}

		count++
		if count%1000 == 0 {
			fmt.Printf("Processed %d provider references (Skipped NPIs: %d, Malformed TINs: %d, Skipped Refs: %d)...\n",
				count, atomic.LoadInt64(&skippedNPIs), atomic.LoadInt64(&malformedTINs), atomic.LoadInt64(&skippedRefs))
			// Force flush every 1000 records
			if err := writer.Flush(); err != nil {
				return fmt.Errorf("failed to flush writer: %v", err)
			}
		}
	}

	// Final flush
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %v", err)
	}

	fmt.Printf("Completed processing %d provider references\n"+
		"Total skipped NPIs: %d\n"+
		"Total malformed TINs: %d\n"+
		"Total skipped references: %d\n",
		count, atomic.LoadInt64(&skippedNPIs), atomic.LoadInt64(&malformedTINs), atomic.LoadInt64(&skippedRefs))
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

	// Even if billing code is nil, try to generate a placeholder
	billingCode := "UNKNOWN_CODE"
	if item.BillingCode != nil {
		billingCode = *item.BillingCode
	}

	// Use empty string for missing modifiers
	billingCodeType := ""
	if item.BillingCodeType != nil {
		billingCodeType = *item.BillingCodeType
	}

	billingCodeVersion := ""
	if item.BillingCodeTypeVersion != nil {
		billingCodeVersion = *item.BillingCodeTypeVersion
	}

	// Log all items for debugging
	if count%1000 == 0 {
		fmt.Printf("Processing item %d - Code: %s, Type: %s, Version: %s\n",
			count, billingCode, billingCodeType, billingCodeVersion)
	}

	// Don't skip empty negotiated rates, just log
	if len(item.NegotiatedRates) == 0 {
		fmt.Printf("INFO: No negotiated rates found for billing code %s (item %d) - creating empty record\n", billingCode, count)
		// Create a single empty record to preserve the billing code
		record := fmt.Sprintf("0,0.0,%s,%s,%s,%s,%s\n",
			billingCode,
			billingCodeType,
			"", // empty negotiation arrangement
			"", // empty negotiated type
			"") // empty billing class
		filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", billingCode))
		return writerPool.WriteAsync(filename, []byte(record))
	}

	var validRecords []string

	for _, rate := range item.NegotiatedRates {
		// If no provider references, use a default
		if len(rate.ProviderReferences) == 0 {
			rate.ProviderReferences = []int{0} // Use 0 as default provider reference
			fmt.Printf("INFO: Using default provider reference for billing code %s (item %d)\n", billingCode, count)
		}

		// If no negotiated prices, create one with zero value
		if len(rate.NegotiatedPrices) == 0 {
			fmt.Printf("INFO: Creating default price for billing code %s (item %d)\n", billingCode, count)
			defaultPrice := NegotiatedPrice{
				NegotiatedRate: new(float64), // Will be 0.0 by default
			}
			rate.NegotiatedPrices = []NegotiatedPrice{defaultPrice}
		}

		for _, price := range rate.NegotiatedPrices {
			// Use 0.0 as default price if nil
			negotiatedRate := 0.0
			if price.NegotiatedRate != nil {
				// Accept any price, even negative ones
				negotiatedRate = *price.NegotiatedRate
			}

			// Use empty strings for all missing optional fields
			negotiatedType := ""
			if price.NegotiatedType != nil {
				negotiatedType = *price.NegotiatedType
			}

			billingClass := ""
			if price.BillingClass != nil {
				billingClass = *price.BillingClass
			}

			negotiationArrangement := ""
			if item.NegotiationArrangement != nil {
				negotiationArrangement = *item.NegotiationArrangement
			}

			for _, providerRef := range rate.ProviderReferences {
				record := fmt.Sprintf("%d,%f,%s,%s,%s,%s,%s\n",
					providerRef,
					negotiatedRate,
					billingCode,
					billingCodeType,
					negotiationArrangement,
					negotiatedType,
					billingClass)
				validRecords = append(validRecords, record)
			}
		}
	}

	// Always write records, even if some fields are empty
	if len(validRecords) > 0 {
		filename := filepath.Join(outputDir, "in_network", fmt.Sprintf("in_network_%s.csv", billingCode))
		return writerPool.WriteAsync(filename, []byte(strings.Join(validRecords, "")))
	}

	return nil
}

func streamInNetworkItems(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) error {
	// Create in_network directory
	inNetworkDir := filepath.Join(outputDir, "in_network")
	if err := os.MkdirAll(inNetworkDir, 0755); err != nil {
		return fmt.Errorf("failed to create in_network directory: %v", err)
	}

	// Read opening bracket
	t, err := decoder.Token()
	if err != nil {
		if err == io.EOF {
			return nil // Empty array is valid
		}
		return fmt.Errorf("failed to read array start: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected array start, got %v", t)
	}

	// Create a buffered channel for items
	itemChan := make(chan InNetworkItem, processorConfig.NumWorkers*2)
	errChan := make(chan error, processorConfig.NumWorkers)
	var wg sync.WaitGroup
	var itemCount int64
	var errorCount int64

	// Start worker goroutines
	fmt.Printf("Starting %d workers for parallel processing...\n", processorConfig.NumWorkers)
	os.Stdout.Sync() // Force flush stdout

	for i := 0; i < processorConfig.NumWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range itemChan {
				if err := processInNetworkItem(item, outputDir, writerPool, &itemCount); err != nil {
					atomic.AddInt64(&errorCount, 1)
					select {
					case errChan <- err:
					default:
						fmt.Printf("WARNING: Error channel full, dropping error: %v\n", err)
					}
				}
			}
		}()
	}

	// Process items and track performance
	startTime := time.Now()
	lastUpdateTime := startTime
	var decodeErrors int64

	go func() {
		defer close(itemChan)
		for decoder.More() {
			var item InNetworkItem
			if err := decoder.Decode(&item); err != nil {
				atomic.AddInt64(&decodeErrors, 1)
				fmt.Printf("WARNING: Failed to decode in-network item: %v\n", err)

				// Try to skip the malformed object
				depth := 0
				for {
					t, err := decoder.Token()
					if err != nil {
						if err == io.EOF {
							fmt.Printf("WARNING: Unexpected EOF while skipping malformed object\n")
							return
						}
						fmt.Printf("WARNING: Error while skipping malformed object: %v\n", err)
						return
					}
					if delim, ok := t.(json.Delim); ok {
						switch delim {
						case '{', '[':
							depth++
						case '}', ']':
							depth--
							if depth < 0 {
								break
							}
						}
					}
					if depth <= 0 {
						break
					}
				}
				continue
			}

			select {
			case itemChan <- item:
			case err := <-errChan:
				fmt.Printf("ERROR: Worker reported error: %v\n", err)
			}

			// Update performance stats every 5 seconds
			now := time.Now()
			if now.Sub(lastUpdateTime) >= 5*time.Second {
				current := atomic.LoadInt64(&itemCount)
				errors := atomic.LoadInt64(&errorCount)
				decodeErrs := atomic.LoadInt64(&decodeErrors)
				elapsed := now.Sub(startTime).Seconds()
				itemsPerSec := float64(current) / elapsed
				fmt.Printf("\n--- Performance: %.1f items/sec (processed %d items, %d errors, %d decode errors in %.1f seconds) ---\n\n",
					itemsPerSec, current, errors, decodeErrs, elapsed)
				os.Stdout.Sync()
				lastUpdateTime = now
			}
		}

		// Read closing bracket
		t, err := decoder.Token()
		if err != nil {
			if err != io.EOF {
				fmt.Printf("WARNING: Failed to read array end: %v\n", err)
			}
			return
		}
		if delim, ok := t.(json.Delim); !ok || delim != ']' {
			fmt.Printf("WARNING: Expected array end, got %v\n", t)
			return
		}
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Check for any remaining errors
	close(errChan)
	var lastErr error
	for err := range errChan {
		lastErr = err
		fmt.Printf("ERROR: %v\n", err)
	}

	// Final statistics
	totalTime := time.Since(startTime)
	totalCount := atomic.LoadInt64(&itemCount)
	totalErrors := atomic.LoadInt64(&errorCount)
	totalDecodeErrors := atomic.LoadInt64(&decodeErrors)
	itemsPerSec := float64(totalCount) / totalTime.Seconds()

	fmt.Printf("\nProcessing complete:\n"+
		"- Total items processed: %d\n"+
		"- Processing errors: %d\n"+
		"- Decode errors: %d\n"+
		"- Time taken: %.1f seconds\n"+
		"- Average speed: %.1f items/sec\n",
		totalCount, totalErrors, totalDecodeErrors, totalTime.Seconds(), itemsPerSec)
	os.Stdout.Sync()

	if lastErr != nil {
		return fmt.Errorf("completed with errors: %v", lastErr)
	}

	return nil
}

// ProcessFile processes the input file and writes results to the output directory
func ProcessFile(inputFile, outputDir string, r2Config *R2Config, remotePrefix string) error {
	fmt.Printf("Opening input file: %s\n", inputFile)

	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	fmt.Printf("Creating gzip reader\n")
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	fmt.Printf("Creating JSON decoder\n")
	decoder := json.NewDecoder(gzReader)

	// Enable number parsing
	decoder.UseNumber()

	// Create writer pool
	fmt.Printf("Creating writer pool\n")
	writerPool := NewFileWriterPool(maxOpenFiles)
	defer writerPool.Close()

	// Configure R2 if needed
	if r2Config != nil {
		writerPool.r2Config = r2Config
		writerPool.remotePrefix = remotePrefix
	}

	// Read opening brace
	fmt.Printf("Reading initial JSON token\n")
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening brace: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected opening brace, got %v", t)
	}

	// Process file contents
	inNetworkFound := false
	sectionsFound := make(map[string]bool)

	fmt.Printf("\nStarting to read JSON structure:\n")
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}

		key, ok := t.(string)
		if !ok {
			fmt.Printf("WARNING: Non-string key found: %v\n", t)
			continue
		}

		sectionsFound[key] = true
		fmt.Printf("Found section: %s\n", key)

		switch key {
		case "provider_references":
			fmt.Printf("Starting provider_references section...\n")
			if err := streamProviderReferences(decoder, outputDir, writerPool); err != nil {
				return fmt.Errorf("failed to process provider references: %v", err)
			}
		case "in_network":
			inNetworkFound = true
			fmt.Printf("Found in_network section, starting processing...\n")
			if err := streamInNetworkItems(decoder, outputDir, writerPool); err != nil {
				return fmt.Errorf("failed to process in-network items: %v", err)
			}
			fmt.Printf("Completed processing in_network section\n")
		default:
			fmt.Printf("Skipping section: %s\n", key)
			// Try to determine the type of the section
			t, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read value type for section %s: %v", key, err)
			}
			if delim, ok := t.(json.Delim); ok {
				fmt.Printf("Section %s is a %v\n", key, delim)
				// Skip the section
				depth := 1
				for depth > 0 {
					t, err := decoder.Token()
					if err != nil {
						return fmt.Errorf("failed to skip section %s: %v", key, err)
					}
					if delim, ok := t.(json.Delim); ok {
						switch delim {
						case '{', '[':
							depth++
						case '}', ']':
							depth--
						}
					}
				}
			} else {
				fmt.Printf("Section %s has value: %v\n", key, t)
			}
		}
	}

	fmt.Printf("\nJSON Structure Summary:\n")
	fmt.Printf("Sections found in file:\n")
	for section := range sectionsFound {
		fmt.Printf("- %s\n", section)
	}

	if !inNetworkFound {
		fmt.Printf("\nWARNING: No in_network section found in the file\n")
	}

	fmt.Printf("\nProcessing complete\n")
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
