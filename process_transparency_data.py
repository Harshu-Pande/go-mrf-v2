import json
import gzip
import pandas as pd
import os
from pathlib import Path
import time
import ijson  # For streaming JSON parsing

def process_provider_references(provider_refs):
    """
    Process provider references into a flat DataFrame
    """
    print("Processing provider references...")
    rows = []
    for ref in provider_refs:
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

def process_in_network_item(item):
    """
    Process a single in_network item into a DataFrame
    """
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
    
    return pd.DataFrame(rows)

def process_transparency_file(input_file, output_dir):
    """
    Process the gzipped JSON file and create CSV files
    """
    start_time = time.time()
    print(f"Starting to process file: {input_file}")
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Process using streaming parser
    with gzip.open(input_file, 'rb') as gz_file:
        # First process provider references
        print("Processing provider references section...")
        provider_refs = []
        parser = ijson.items(gz_file, 'provider_references.item')
        for ref in parser:
            provider_refs.append(ref)
            if len(provider_refs) >= 1000:
                df = process_provider_references(provider_refs)
                df.to_csv(f"{output_dir}/provider_references.csv", 
                         mode='a',
                         header=not os.path.exists(f"{output_dir}/provider_references.csv"),
                         index=False)
                provider_refs = []
                print(f"Processed {len(df)} provider reference rows")
        
        if provider_refs:
            df = process_provider_references(provider_refs)
            df.to_csv(f"{output_dir}/provider_references.csv", 
                     mode='a',
                     header=not os.path.exists(f"{output_dir}/provider_references.csv"),
                     index=False)
            print(f"Processed final {len(df)} provider reference rows")
        
        # Reset file pointer for in_network processing
        gz_file.seek(0)
        
        # Now process in_network items
        print("\nProcessing in-network section...")
        billing_codes_seen = set()
        item_count = 0
        parser = ijson.items(gz_file, 'in_network.item')
        
        for item in parser:
            item_count += 1
            if item_count % 100 == 0:
                print(f"Processed {item_count} in-network items...")
            
            billing_code = item['billing_code']
            df = process_in_network_item(item)
            output_file = f"{output_dir}/in_network_{billing_code}.csv"
            
            df.to_csv(output_file, 
                     mode='a' if billing_code in billing_codes_seen else 'w',
                     header=not (billing_code in billing_codes_seen),
                     index=False)
            
            billing_codes_seen.add(billing_code)
    
    print(f"\nTotal processing time: {time.time() - start_time:.2f} seconds")
    print(f"Total in-network items processed: {item_count}")
    print(f"Total unique billing codes: {len(billing_codes_seen)}")

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