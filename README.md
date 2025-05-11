# MRF JSON Processor

A high-performance Go program for processing Machine-Readable Files (MRF) from healthcare price transparency data. This tool efficiently processes large gzipped JSON files using parallel processing, memory-mapped I/O, and streaming techniques.

## Features

- Parallel processing using configurable worker threads
- Memory-efficient streaming JSON processing
- Memory-mapped I/O for better performance
- Optimized file writing with buffer pools
- Automatic system resource limit optimization
- Handles large gzipped files
- Configurable buffer sizes and worker counts
- CSV output format for easy analysis

## Requirements

- Go 1.16 or later
- Unix-like operating system (Linux, macOS)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/mrf-processor
cd mrf-processor
```

2. Build the program:
```bash
go build -o mrf-processor
```

## Usage

```bash
./mrf-processor [options] <input_file.json.gz> <output_dir>

Options:
  -workers int
        Number of worker threads (default: number of CPU cores)
  -buffer int
        Write buffer size in MB (default: 1)
```

Example:
```bash
./mrf-processor -workers 8 -buffer 4 input.json.gz output/
```

## Output Format

The program generates two CSV files in the output directory:

1. `provider_groups.csv`:
   - npi: National Provider Identifier
   - tin_type: Tax Identification Number type
   - tin_value: Tax Identification Number value

2. `negotiated_rates.csv`:
   - provider_reference: Reference to the provider group
   - negotiated_rate: The negotiated price
   - billing_code: Service billing code
   - billing_code_type: Type of billing code
   - negotiation_arrangement: Type of negotiation arrangement
   - negotiated_type: Type of negotiated rate
   - billing_class: Billing class

## Performance Features

The program is highly optimized for performance:
- Uses memory-mapped I/O for efficient file handling
- Implements parallel processing with configurable worker threads
- Uses buffer pools for optimized file writing
- Automatically adjusts system resource limits
- Streams JSON data to minimize memory usage
- Uses efficient data structures for processing

## License

MIT License - See LICENSE file for details # med-reveal-go
