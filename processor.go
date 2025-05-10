package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
)

// ProcessorConfig holds configuration for the processor
type ProcessorConfig struct {
	WriteBufferSize int
	NumWorkers      int
}

// DefaultConfig returns the default processor configuration
func DefaultConfig() ProcessorConfig {
	return ProcessorConfig{
		WriteBufferSize: 1024 * 1024, // 1MB write buffer
		NumWorkers:      runtime.NumCPU(),
	}
}

var config ProcessorConfig

// SetConfig sets the processor configuration
func SetConfig(cfg ProcessorConfig) {
	config = cfg
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
	file    *os.File
	writer  *bufio.Writer
	mu      sync.Mutex
	written int64
}

// Pool of FileWriters
type FileWriterPool struct {
	writers map[string]*FileWriter
	mu      sync.RWMutex
}

func (fw *FileWriter) Write(data []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	n, err := fw.writer.Write(data)
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

func (pool *FileWriterPool) GetWriter(filename string) (*FileWriter, error) {
	pool.mu.RLock()
	writer, exists := pool.writers[filename]
	pool.mu.RUnlock()
	if exists {
		return writer, nil
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	// Check again in case another goroutine created it
	if writer, exists = pool.writers[filename]; exists {
		return writer, nil
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %v", filename, err)
	}

	writer = &FileWriter{
		file:   file,
		writer: bufio.NewWriterSize(file, config.WriteBufferSize),
	}
	pool.writers[filename] = writer
	return writer, nil
}

func (pool *FileWriterPool) Close() error {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	var lastErr error
	for _, writer := range pool.writers {
		if err := writer.Flush(); err != nil {
			lastErr = err
		}
		if err := writer.file.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func writeProviderGroups(group ProviderGroup, outputDir string, writerPool *FileWriterPool) error {
	filename := filepath.Join(outputDir, "provider_groups.csv")
	writer, err := writerPool.GetWriter(filename)
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

	record := fmt.Sprintf("%s,%s,%s\n",
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
			if err := writeProviderGroups(group, outputDir, writerPool); err != nil {
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
	filename := filepath.Join(outputDir, "negotiated_rates.csv")
	writer, err := writerPool.GetWriter(filename)
	if err != nil {
		return fmt.Errorf("failed to get writer for negotiated rates: %v", err)
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
	fmt.Printf("Processing in-network item %d: %s\n", count, item.BillingCode)

	for _, rate := range item.NegotiatedRates {
		for _, price := range rate.NegotiatedPrices {
			for _, providerRef := range rate.ProviderReferences {
				if err := writeNegotiatedRates(item, rate, price, providerRef, outputDir, writerPool); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func streamInNetworkItems(decoder *json.Decoder, outputDir string, writerPool *FileWriterPool) error {
	// Create a buffered channel for items
	itemChan := make(chan InNetworkItem, config.NumWorkers*2)
	errChan := make(chan error, 1)
	var wg sync.WaitGroup
	var itemCount int64

	// Start worker goroutines
	fmt.Printf("Starting %d workers for parallel processing...\n", config.NumWorkers)

	for i := 0; i < config.NumWorkers; i++ {
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

	// Read items and send to channel
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
		}
	}()

	// Wait for all workers to finish
	wg.Wait()

	// Check for errors
	select {
	case err := <-errChan:
		return err
	default:
	}

	// Consume the array end token
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("failed to read array end token: %v", err)
	}

	fmt.Printf("Processed %d in-network items using %d workers\n", atomic.LoadInt64(&itemCount), config.NumWorkers)
	return nil
}

func ProcessFile(inputFile, outputDir string) error {
	fmt.Printf("Starting processing with %d CPU cores...\n", runtime.NumCPU())

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Initialize the writer pool
	writerPool := &FileWriterPool{
		writers: make(map[string]*FileWriter),
	}
	defer writerPool.Close()

	// Create CSV files with headers
	headers := map[string]string{
		"provider_groups.csv":  "npi,tin_type,tin_value\n",
		"negotiated_rates.csv": "provider_reference,negotiated_rate,billing_code,billing_code_type,negotiation_arrangement,negotiated_type,billing_class\n",
	}

	for filename, header := range headers {
		writer, err := writerPool.GetWriter(filepath.Join(outputDir, filename))
		if err != nil {
			return fmt.Errorf("failed to create %s: %v", filename, err)
		}
		if _, err := writer.Write([]byte(header)); err != nil {
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
