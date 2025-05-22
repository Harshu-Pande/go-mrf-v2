# MRF JSON Processor

A high-performance Go program for processing Machine-Readable Files (MRF) from healthcare price transparency data. This tool efficiently processes large gzipped JSON files using parallel processing, memory-mapped I/O, and streaming techniques.

## Features

- Parallel processing using configurable worker threads
- Memory-efficient streaming JSON processing
- Memory-mapped I/O for better performance
- Optimized file writing with buffer pools
- Automatic system resource limit optimization
- Handles large gzipped files (200GB+)
- Configurable buffer sizes and worker counts
- CSV output format for easy analysis
- Optional Cloudflare R2 storage integration

## Requirements

- Go 1.16 or later
- Unix-like operating system (Linux, macOS, or Google Colab)

## Running in Google Colab

1. First, create a new notebook in Google Colab and run these cells in order:

```python
%%shell
# Cell 1: Install Go
rm -rf /usr/local/go
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
go version
```

```python
%%shell
# Cell 2: Clone the repository
git clone https://github.com/Harshu-Pande/go-mrf-v2.git
cd go-mrf-v2
```

```python
%%shell
# Cell 3: Set up environment and run the processor
export PATH=$PATH:/usr/local/go/bin

cd /content/go-mrf-v2
go build -o mrf-processor
mkdir -p output

# For local storage only (results saved in Colab):
./mrf-processor \
  -workers 99 \
  -buffer 4 \
  "/path/to/your/input.json.gz" \
  "/content/output"

# OR for R2 storage (replace with your credentials):
export R2_ACCOUNT_ID="your_account_id"
export R2_ACCESS_KEY_ID="your_access_key"
export R2_ACCESS_KEY_SECRET="your_secret_key"
export R2_BUCKET_NAME="your_bucket_name"

./mrf-processor \
  -workers 99 \
  -buffer 4 \
  -r2-upload \
  -r2-prefix "your/prefix/path" \
  -keep-local=true \  # Set to false to delete local files after upload
  "/path/to/your/input.json.gz" \
  "/content/output"
```

### Storage Options

#### Local Storage
- Use `-keep-local=true` (default) to keep processed files in the Colab environment
- Files will be saved in the specified output directory
- Access the files through Colab's file browser or download them

#### Cloudflare R2 Storage
To upload results to R2:
1. Set up R2 environment variables:
   ```bash
   export R2_ACCOUNT_ID="your_account_id"
   export R2_ACCESS_KEY_ID="your_access_key"
   export R2_ACCESS_KEY_SECRET="your_secret_key"
   export R2_BUCKET_NAME="your_bucket_name"
   ```
2. Use these flags:
   - `-r2-upload`: Enable R2 upload
   - `-r2-prefix "path/prefix"`: Set the prefix for files in R2 bucket
   - `-keep-local=false`: Optionally delete local files after successful upload

## Command Line Options

```bash
./mrf-processor [options] <input_file.json.gz> <output_dir>

Options:
  -workers int
        Number of worker threads (default: number of CPU cores)
  -buffer int
        Write buffer size in MB (default: 1)
  -r2-upload
        Enable upload to R2 storage
  -r2-prefix string
        Prefix for files in R2 bucket
  -keep-local
        Keep local files after R2 upload (default: true)
```

## Output Format

The program generates two types of files:

1. `provider_references/provider_groups.csv`:
   - provider_group_id: Unique identifier for the provider group
   - npi: National Provider Identifier
   - tin_type: Tax Identification Number type
   - tin_value: Tax Identification Number value

2. `in_network/in_network_{billing_code}.csv`:
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
- Handles files over 200GB in size efficiently

## License

MIT License - See LICENSE file for details # med-reveal-go
