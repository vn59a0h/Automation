import pandas as pd
import os

def create_bucket(dag_config, dlSchemaName, output_file="../buckets/bucket_input.csv", env='prod'):
    """
    Create a bucket input CSV row from dag_config for bucket creation request
    
    Args:
        dag_config: Dictionary with configuration values
        dlSchemaName: Schema name (e.g., 'sa_mdse_dl_tables')
        output_file: Path to append/create bucket_input.csv
        env: Environment (default 'prod')
        
    Returns:
        str: Path to the bucket_input.csv file
    """
    print("Creating bulk bucket ... ")
    try:
        import pandas as pd
        
        # Map sensitivity codes to service account suffix
        sensitivity_map = {
            'se': 'ns',
            'ns': 'ns',
            'hs': 'hs'
        }
        
        table_name = dag_config.get('table_name')
        banner_name = dag_config.get('banner_name')
        sensitivity = dag_config.get('sensitivity', 'ns').lower()
        load_type = dag_config.get('tableLoadType', 'incremental').lower()
        
        sa_suffix = sensitivity_map.get(sensitivity, 'ns')
        op_cmpny_cd = f"SA-{banner_name.upper()}"
        service_account = f"svc-dl-sa-afaas-{sa_suffix}@wmt-intl-dl-sa-{sa_suffix}-{env}.iam.gserviceaccount.com"
        refresh_mode = "incremental load" if load_type == "inc" else f"{load_type} load"
        
        bucket_row = {
            'bucketNameType': 'hash',
            'databaseName': dlSchemaName,
            'tableName': table_name,
            'opCmpnyCd': op_cmpny_cd,
            'refreshMode': refresh_mode,
            'wmt.storage_uploader': service_account,
            'wmt.storage_viewer': service_account,
            'isDevBigLake': 'FALSE',
            'updateSoftDelete': 'DCA Logic',
            'resourceBucketType': ''
        }
        
        file_exists = os.path.exists(output_file)
        df_row = pd.DataFrame([bucket_row])
        
        if file_exists and os.path.getsize(output_file) > 0:
            df_existing = pd.read_csv(output_file)
            df_combined = pd.concat([df_existing, df_row], ignore_index=True)
            df_combined.to_csv(output_file, index=False)
        else:
            df_row.to_csv(output_file, index=False)
        
        print(f"✓ Added bucket entry for {table_name} to {output_file}")
        return output_file
        
    except Exception as e:
        print(f"Error creating bucket entry: {e}")
        raise

def extract_bucket_id(final_bucket_info_file="../buckets/FinalBucketInfo.csv", output_file="../buckets/bucket_id.csv"):
    """
    Extract table name and bucket id from FinalBucketInfo.csv and create bucket_id.csv
    
    Args:
        final_bucket_info_file: Path to the source FinalBucketInfo.csv file
        output_file: Path to save the output bucket_id.csv file
        
    Returns:
        str: Path to the generated bucket_id.csv file
        
    Raises:
        FileNotFoundError: If FinalBucketInfo.csv is not found
    """
    
    try:
        # Read the FinalBucketInfo.csv file
        final_bucket_df = pd.read_csv(final_bucket_info_file)
        
        # Extract required columns
        bucket_mapping = final_bucket_df[['tableName', 'bucket_name']].copy()
        
        # Rename columns to match expected format
        bucket_mapping.columns = ['dlTableName', 'bucket_id']
        
        # Convert tableName to lowercase format
        bucket_mapping['dlTableName'] = bucket_mapping['dlTableName'].str.lower()
        
        # Remove duplicates if any
        bucket_mapping = bucket_mapping.drop_duplicates(subset=['dlTableName'])
        
        # Save to bucket_id.csv
        bucket_mapping.to_csv(output_file, index=False)
        
        print(f"✓ Successfully created {output_file}")
        print(f"✓ Extracted {len(bucket_mapping)} table-bucket mappings")
        print(f"\nSample mappings:")
        print(bucket_mapping.head())
        
        return output_file
        
    except FileNotFoundError:
        print(f"Error: {final_bucket_info_file} not found")
        raise
    except Exception as e:
        print(f"Error processing file: {e}")
        raise

def get_bucket_id(dag_config):
    """
    Read bucket id from a bucket_id.csv file based on table name
    
    CSV file format:
    dlTableName,bucket_id
    msb_physl_invt_doc,86009702bc2c3bcd2c1c64cfb8e9a2e7ed7046d5e4b77786956ff1fe50586d
    mak_physl_invt_doc,abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
    
    Args:
        dag_config: Dictionary containing 'table_name' key
        
    Returns:
        bucket_id: The GCS bucket ID for the table
        
    Raises:
        FileNotFoundError: If bucket_id.csv is not found
        ValueError: If table_name is not found in the CSV
    """
    
    try:
        bucket_csv_file = "../buckets/bucket_id.csv"
        
        # Read the CSV file
        bucket_df = pd.read_csv(bucket_csv_file)
        
        # Extract table name from dag_config
        table_name = dag_config.get('table_name')
        
        if not table_name:
            raise ValueError("table_name not found in dag_config")
        
        # Find matching row
        matching_rows = bucket_df[bucket_df['dlTableName'] == table_name]
        
        if matching_rows.empty:
            raise ValueError(f"Table '{table_name}' not found in {bucket_csv_file}")
        
        # Get bucket_id from the first matching row
        bucket_id = matching_rows.iloc[0]['bucket_id']
        
        print(f"✓ Retrieved bucket_id for {table_name}: {bucket_id}")
        return bucket_id
        
    except FileNotFoundError:
        print(f"Error: {bucket_csv_file} not found")
        raise
    except ValueError as e:
        print(f"Error: {e}")
        raise