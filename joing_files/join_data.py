import duckdb
import os

# Define file paths
npi_file = "/Users/harshupande/Desktop/very_fast_json/npi_zip_63-2.csv"
provider_groups_file = "/Users/harshupande/Desktop/very_fast_json/provider_groups-2.csv"
in_network_file = "/Users/harshupande/Desktop/very_fast_json/in_network_0002M.csv"

# Initialize DuckDB
con = duckdb.connect(':memory:')

# Create SQL query to join the data
query = """
WITH npi_providers AS (
    -- First, get relevant fields from the NPI file
    SELECT DISTINCT 
        CAST(NPI AS VARCHAR) as npi,
        "Provider Organization Name (Legal Business Name)" as org_name,
        "Provider Last Name (Legal Name)" as last_name,
        "Provider First Name" as first_name,
        "Provider Business Practice Location Address City Name" as city,
        "Provider Business Practice Location Address State Name" as state,
        "Provider Business Practice Location Address Postal Code" as zip,
        "Provider Business Practice Location Address Telephone Number" as phone,
        "Healthcare Provider Taxonomy Code_1" as taxonomy_code
    FROM read_csv_auto(?)
),
matching_provider_groups AS (
    -- Then match these NPIs with provider_groups and get their provider_group_ids
    SELECT DISTINCT pg.provider_group_id, pg.npi, pg.tin_type, pg.tin_value
    FROM read_csv_auto(?) pg
    INNER JOIN npi_providers np ON pg.npi = np.npi
),
final_rates AS (
    -- Finally, get the negotiated rates for these provider_group_ids
    SELECT 
        np.*,
        mpg.provider_group_id,
        mpg.tin_type,
        mpg.tin_value,
        ir.negotiated_rate,
        ir.billing_code,
        ir.billing_code_type,
        ir.negotiation_arrangement,
        ir.negotiated_type,
        ir.billing_class
    FROM read_csv_auto(?) ir
    INNER JOIN matching_provider_groups mpg ON ir.provider_reference = mpg.provider_group_id
    INNER JOIN npi_providers np ON mpg.npi = np.npi
)
SELECT * FROM final_rates
ORDER BY npi, negotiated_rate;
"""

# Execute the query
result = con.execute(query, [npi_file, provider_groups_file, in_network_file]).fetchdf()

# Print summary statistics
print("\nSummary:")
print(f"Total matching records: {len(result)}")
print(f"Unique NPIs: {result['npi'].nunique()}")
print(f"Unique provider_group_ids: {result['provider_group_id'].nunique()}")
print("\nSample of results (first 10 rows):")
print(result.head(10))

# Save the results
output_file = "matched_rates_with_npi_info.csv"
result.to_csv(output_file, index=False)
print(f"\nFull results saved to: {output_file}")

# Print column names for reference
print("\nAvailable columns in the output:")
for col in result.columns:
    print(f"- {col}") 