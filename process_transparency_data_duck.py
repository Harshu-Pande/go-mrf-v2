import duckdb
import json
import gzip
from pathlib import Path
import time
import psutil

def process_transparency_file(input_file: str, output_dir: str):
    """
    Process the transparency file using DuckDB for high-performance analytics
    """
    start_time = time.time()
    print(f"Starting processing with DuckDB...")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Initialize DuckDB connection with optimized settings
    con = duckdb.connect(':memory:')
    # Configure DuckDB for better performance
    total_memory = psutil.virtual_memory().total
    memory_limit = int(total_memory * 0.75)  # Use 75% of available memory
    con.execute(f"SET memory_limit='{memory_limit}B'")
    con.execute("SET threads=8")  # Adjust based on available CPU cores
    
    print("Reading and parsing file...")
    # First read the gzipped file content
    with gzip.open(input_file, 'rt') as f:
        json_str = f.read()
    
    # Load JSON into DuckDB
    con.execute("CREATE TABLE raw_data AS SELECT * FROM read_json_auto(?);", [json_str])
    print("File loaded into DuckDB")

    # Process provider references
    print("\nProcessing provider references...")
    provider_query = """
    WITH provider_data AS (
        SELECT 
            pr.provider_group_id,
            UNNEST(pg.npi) as npi,
            pg.tin->>'type' as tin_type,
            pg.tin->>'value' as tin_value
        FROM raw_data,
        UNNEST(provider_references) AS pr,
        UNNEST(pr.provider_groups) AS pg
    )
    SELECT * FROM provider_data;
    """
    
    print("Saving provider references...")
    con.execute(f"""
    COPY ({provider_query}) 
    TO '{output_dir}/provider_references.csv' 
    (FORMAT CSV, HEADER);
    """)
    
    provider_count = con.execute("SELECT COUNT(*) FROM (" + provider_query + ")").fetchone()[0]
    print(f"Saved {provider_count} provider reference rows")

    # Process in-network items with only needed columns
    print("\nProcessing in-network items...")
    in_network_base_query = """
    WITH flattened_data AS (
        SELECT 
            i.billing_code,
            i.billing_code_type,
            i.negotiation_arrangement,
            UNNEST(i.negotiated_rates) as rate
        FROM raw_data,
        UNNEST(in_network) AS i
    )
    SELECT 
        billing_code,
        billing_code_type,
        negotiation_arrangement,
        UNNEST(rate.provider_references) as provider_reference,
        p.negotiated_rate,
        p.negotiated_type,
        p.billing_class
    FROM flattened_data,
    UNNEST(rate.negotiated_prices) AS p;
    """

    # Create an optimized table for the flattened data
    print("Creating optimized table for in-network data...")
    con.execute(f"""
    CREATE TABLE flattened_in_network AS 
    {in_network_base_query}
    """)

    # Get unique billing codes
    billing_codes = con.execute("""
    SELECT DISTINCT billing_code 
    FROM flattened_in_network
    ORDER BY billing_code
    """).fetchall()
    
    # Process each billing code
    print("Saving in-network data by billing code...")
    total_rows = 0
    for idx, (code,) in enumerate(billing_codes, 1):
        output_file = f"{output_dir}/in_network_{code}.csv"
        
        # Export data for this billing code
        con.execute(f"""
        COPY (
            SELECT * 
            FROM flattened_in_network 
            WHERE billing_code = '{code}'
        ) TO '{output_file}' (FORMAT CSV, HEADER);
        """)
        
        # Get count for this billing code
        count = con.execute(f"""
        SELECT COUNT(*) 
        FROM flattened_in_network 
        WHERE billing_code = '{code}'
        """).fetchone()[0]
        total_rows += count
        
        if idx % 10 == 0 or idx == len(billing_codes):
            print(f"Progress: {idx}/{len(billing_codes)} billing codes processed ({total_rows} total rows)")

    total_time = time.time() - start_time
    print(f"\nTotal processing time: {total_time:.2f} seconds")
    print(f"Total unique billing codes processed: {len(billing_codes)}")
    print(f"Total rows processed: {total_rows}")

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "Raw_Files/UHC_Nevada_HMO_5_1_25.json.gz"
    OUTPUT_DIR = "Processed_Files"
    
    try:
        process_transparency_file(INPUT_FILE, OUTPUT_DIR)
        print("Processing completed successfully!")
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        print(traceback.format_exc()) 