package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Only the structures we need
type ProviderGroup struct {
	NPI interface{} `json:"npi"` // Can be either string or []string
	TIN struct {
		Type  string `json:"type"`
		Value string `json:"value"`
	} `json:"tin"`
}

type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
}

type NegotiatedPrice struct {
	NegotiatedRate float64 `json:"negotiated_rate"`
	NegotiatedType string  `json:"negotiated_type"`
	BillingClass   string  `json:"billing_class"`
}

type NegotiatedRate struct {
	ProviderReferences []int             `json:"provider_references"`
	NegotiatedPrices   []NegotiatedPrice `json:"negotiated_prices"`
}

type InNetworkItem struct {
	BillingCode            string           `json:"billing_code"`
	BillingCodeType        string           `json:"billing_code_type"`
	NegotiationArrangement string           `json:"negotiation_arrangement"`
	NegotiatedRates        []NegotiatedRate `json:"negotiated_rates"`
}

type ProcessingJob struct {
	Code  string
	Items []InNetworkItem
}

func processProviderReferences(provRefs []ProviderReference, outputDir string) error {
	outputFile := filepath.Join(outputDir, "provider_references.csv")
	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	writer := csv.NewWriter(bufWriter)
	defer func() {
		writer.Flush()
		bufWriter.Flush()
	}()

	// Write header
	writer.Write([]string{"provider_group_id", "npi", "tin_type", "tin_value"})

	for _, ref := range provRefs {
		for _, group := range ref.ProviderGroups {
			// Handle different NPI types
			var npis []string
			switch npi := group.NPI.(type) {
			case string:
				npis = []string{npi}
			case []interface{}:
				npis = make([]string, 0, len(npi))
				for _, n := range npi {
					if str, ok := n.(string); ok {
						npis = append(npis, str)
					} else if num, ok := n.(float64); ok {
						npis = append(npis, fmt.Sprintf("%d", int(num)))
					}
				}
			case float64:
				npis = []string{fmt.Sprintf("%d", int(npi))}
			}

			for _, npi := range npis {
				writer.Write([]string{
					fmt.Sprintf("%d", ref.ProviderGroupID),
					npi,
					group.TIN.Type,
					group.TIN.Value,
				})
			}
		}
	}
	return nil
}

func processInNetworkJob(job ProcessingJob, outputDir string) error {
	outputFile := filepath.Join(outputDir, fmt.Sprintf("in_network_%s.csv", job.Code))
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("error creating file for code %s: %v", job.Code, err)
	}
	defer file.Close()

	bufWriter := bufio.NewWriterSize(file, 1024*1024) // 1MB buffer
	writer := csv.NewWriter(bufWriter)
	defer func() {
		writer.Flush()
		bufWriter.Flush()
	}()

	// Write header
	writer.Write([]string{
		"billing_code",
		"billing_code_type",
		"negotiation_arrangement",
		"provider_reference",
		"negotiated_rate",
		"negotiated_type",
		"billing_class",
	})

	// Pre-allocate a reusable row buffer
	row := make([]string, 7)

	// Write data rows
	for _, item := range job.Items {
		row[0] = item.BillingCode
		row[1] = item.BillingCodeType
		row[2] = item.NegotiationArrangement

		for _, rate := range item.NegotiatedRates {
			for _, provRef := range rate.ProviderReferences {
				row[3] = fmt.Sprintf("%d", provRef)

				for _, price := range rate.NegotiatedPrices {
					row[4] = fmt.Sprintf("%f", price.NegotiatedRate)
					row[5] = price.NegotiatedType
					row[6] = price.BillingClass
					writer.Write(row)
				}
			}
		}
	}
	return nil
}

func worker(id int, jobs <-chan ProcessingJob, results chan<- error, outputDir string) {
	for job := range jobs {
		err := processInNetworkJob(job, outputDir)
		results <- err
	}
}

func processInNetworkItems(items []InNetworkItem, outputDir string) error {
	// Group items by billing code
	itemsByCode := make(map[string][]InNetworkItem)
	for _, item := range items {
		itemsByCode[item.BillingCode] = append(itemsByCode[item.BillingCode], item)
	}

	numWorkers := runtime.NumCPU() // Use number of CPU cores for parallelism
	jobs := make(chan ProcessingJob, numWorkers)
	results := make(chan error, len(itemsByCode))

	// Start worker pool
	var wg sync.WaitGroup
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(id, jobs, results, outputDir)
		}(w)
	}

	// Send jobs to workers
	go func() {
		for code, codeItems := range itemsByCode {
			jobs <- ProcessingJob{
				Code:  code,
				Items: codeItems,
			}
		}
		close(jobs)
	}()

	// Wait for all jobs to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Check for errors
	processed := 0
	total := len(itemsByCode)
	for err := range results {
		processed++
		if processed%100 == 0 || processed == total {
			fmt.Printf("Processed %d/%d billing codes\n", processed, total)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: process_mrf <input_file> <output_dir>")
		os.Exit(1)
	}

	inputFile := os.Args[1]
	outputDir := os.Args[2]

	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	startTime := time.Now()

	// Open the gzipped file
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create a gzip reader
	gz, err := gzip.NewReader(file)
	if err != nil {
		fmt.Printf("Error creating gzip reader: %v\n", err)
		os.Exit(1)
	}
	defer gz.Close()

	// Create a buffered reader for better performance
	reader := bufio.NewReaderSize(gz, 1024*1024) // 1MB buffer

	// Read opening brace
	reader.ReadByte()

	var providerRefs []ProviderReference
	var inNetworkItems []InNetworkItem

	// Process the file
	decoder := json.NewDecoder(reader)

	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error decoding JSON: %v\n", err)
			os.Exit(1)
		}

		// Look for field names
		if str, ok := token.(string); ok {
			switch str {
			case "provider_references":
				if err := decoder.Decode(&providerRefs); err != nil {
					fmt.Printf("Error decoding provider references: %v\n", err)
					os.Exit(1)
				}
				fmt.Printf("Found %d provider references\n", len(providerRefs))

			case "in_network":
				if err := decoder.Decode(&inNetworkItems); err != nil {
					fmt.Printf("Error decoding in-network items: %v\n", err)
					os.Exit(1)
				}
				fmt.Printf("Found %d in-network items\n", len(inNetworkItems))
			}
		}
	}

	// Process the data concurrently
	var wg sync.WaitGroup
	wg.Add(2)

	var provErr, netErr error
	go func() {
		defer wg.Done()
		if err := processProviderReferences(providerRefs, outputDir); err != nil {
			provErr = fmt.Errorf("error processing provider references: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := processInNetworkItems(inNetworkItems, outputDir); err != nil {
			netErr = fmt.Errorf("error processing in-network items: %v", err)
		}
	}()

	wg.Wait()

	if provErr != nil {
		fmt.Println(provErr)
	}
	if netErr != nil {
		fmt.Println(netErr)
	}

	elapsed := time.Since(startTime)
	fmt.Printf("Processing completed in %s\n", elapsed)
}
