import requests
import os
from pathlib import Path

def download_gzipped_json(url: str, output_path: str) -> None:
    """
    Download a gzipped JSON file and save it in its compressed format.
    
    Args:
        url (str): URL of the gzipped JSON file
        output_path (str): Path where the file should be saved
    """
    # Create the directory if it doesn't exist
    output_dir = os.path.dirname(output_path)
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Download the file with streaming to handle large files efficiently
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an exception for bad status codes
    
    # Write the compressed content to file
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Successfully downloaded file to: {output_path}")

if __name__ == "__main__":
    # Example usage - you can modify these values as needed
    URL = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-05-01/2025-05-01_Sierra-Health---Life---Nevada_Insurer_Commercial-HMO_Commercial-HMO_in-network-rates.json.gz?undefined"
    OUTPUT_PATH = "/Users/harshupande/Desktop/very_fast_json/Raw_Files/UHC_Nevada_HMO_5_1_25.json.gz"
    
    try:
        download_gzipped_json(URL, OUTPUT_PATH)
    except Exception as e:
        print(f"Error downloading file: {e}") 