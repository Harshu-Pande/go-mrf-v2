# MRF JSON Processor

A high-performance Go program for processing Machine-Readable Files (MRF) from healthcare price transparency data. This tool efficiently processes large gzipped JSON files using parallel processing and streaming techniques.

## Features

- Parallel processing using all available CPU cores
- Memory-efficient streaming JSON processing
- Handles large gzipped files
- Configurable input/output paths
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
./mrf-processor input.json.gz output/
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

## Performance

The program is optimized for performance:
- Uses parallel processing with multiple worker threads
- Streams JSON data to minimize memory usage
- Buffers file writes for better I/O performance
- Automatically uses all available CPU cores

## License

MIT License - See LICENSE file for details # med-reveal-go
