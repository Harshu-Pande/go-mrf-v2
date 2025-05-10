import json
import gzip
import pandas as pd
import os
from pathlib import Path
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import numpy as np
from typing import List, Dict, Any
import warnings
warnings.filterwarnings('ignore')

def chunk_list(lst: list, chunk_size: int) -> List[list]:
    """Split a list into chunks of specified size"""
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def process_provider_chunk(chunk: List[Dict]) -> pd.DataFrame:
    """Process a chunk of provider references"""
    rows = []
    for ref in chunk:
        provider_group_id = ref['provider_group_id']
        for provider_group in ref['provider_groups']:
            for npi in provider_group['npi']:
                rows.append({
                    'provider_group_id': provider_group_id,
                    'npi': npi,
                    'tin_type': provider_group['tin']['type'],
                    'tin_value': provider_group['tin']['value']
                })
    return pd.DataFrame(rows)

def process_in_network_chunk(chunk: List[Dict]) -> List[pd.DataFrame]:
    """Process a chunk of in_network items"""
    results = []
    for item in chunk:
        rows = []
        base_info = {
            'billing_code': item['billing_code'],
            'billing_code_type': item['billing_code_type'],
            'name': item['name'],
            'description': item['description'],
            'negotiation_arrangement': item['negotiation_arrangement']
        }
        
        for rate in item['negotiated_rates']:
            for provider_ref in rate['provider_references']:
                for price in rate['negotiated_prices']:
                    row = base_info.copy()
                    row.update({
                        'provider_reference': provider_ref,
                        'negotiated_rate': price['negotiated_rate'],
                        'service_codes': ','.join(price['service_code']),
                        'negotiated_type': price['negotiated_type'],
                        'expiration_date': price['expiration_date'],
                        'billing_class': price['billing_class']
                    })
                    rows.append(row)
        
        if rows:
            df = pd.DataFrame(rows)
            results.append((item['billing_code'], df))
    
    return results

def parallel_process_file(input_file: str, output_dir: str, n_workers: int = None):
    """
    Process the transparency file using parallel processing
    """
    if n_workers is None:
        n_workers = mp.cpu_count()
    
    start_time = time.time()
    print(f"Starting parallel processing with {n_workers} workers")
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Read the entire file into memory (since we're on Colab with lots of RAM)
    print("Loading file into memory...")
    with gzip.open(input_file, 'rt') as f:
        data = json.load(f)
    
    # Process provider references in parallel
    print("\nProcessing provider references...")
    provider_refs = data['provider_references']
    chunk_size = max(1000, len(provider_refs) // (n_workers * 4))  # Ensure enough chunks for parallelization
    chunks = chunk_list(provider_refs, chunk_size)
    
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        dfs = list(executor.map(process_provider_chunk, chunks))
    
    # Combine and save provider references
    print("Combining provider reference results...")
    final_provider_df = pd.concat(dfs, ignore_index=True)
    final_provider_df.to_csv(f"{output_dir}/provider_references.csv", index=False)
    print(f"Saved {len(final_provider_df)} provider reference rows")
    
    # Process in_network items in parallel
    print("\nProcessing in-network items...")
    in_network_items = data['in_network']
    chunk_size = max(100, len(in_network_items) // (n_workers * 4))
    chunks = chunk_list(in_network_items, chunk_size)
    
    # Clear memory
    del data
    
    billing_code_dfs = {}
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        for chunk_results in executor.map(process_in_network_chunk, chunks):
            for billing_code, df in chunk_results:
                if billing_code not in billing_code_dfs:
                    billing_code_dfs[billing_code] = []
                billing_code_dfs[billing_code].append(df)
    
    # Save in-network results
    print("\nSaving in-network results...")
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = []
        for billing_code, dfs in billing_code_dfs.items():
            final_df = pd.concat(dfs, ignore_index=True)
            output_file = f"{output_dir}/in_network_{billing_code}.csv"
            futures.append(
                executor.submit(final_df.to_csv, output_file, index=False)
            )
        
        # Wait for all saves to complete
        for future in futures:
            future.result()
    
    total_time = time.time() - start_time
    print(f"\nTotal processing time: {total_time:.2f} seconds")
    print(f"Total unique billing codes processed: {len(billing_code_dfs)}")

if __name__ == "__main__":
    # Configuration
    INPUT_FILE = "Raw_Files/UHC_Nevada_HMO_5_1_25.json.gz"
    OUTPUT_DIR = "Processed_Files"
    
    # For Google Colab, you might want to set this to a higher number based on available cores
    N_WORKERS = mp.cpu_count()  # Uses all available CPU cores
    
    try:
        parallel_process_file(INPUT_FILE, OUTPUT_DIR, N_WORKERS)
        print("Processing completed successfully!")
    except Exception as e:
        print(f"Error processing file: {e}")
        import traceback
        print(traceback.format_exc()) 