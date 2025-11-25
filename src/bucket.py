"""
Bucket management functions for GCS bucket creation and mapping.

Workflow:
1. generate_bucket_input_csv() - Create bucket_input.csv from dataframe
2. (Manual Step) - User creates buckets via portal, gets FinalBucketInfo.csv
3. parse_final_bucket_info() - Parse FinalBucketInfo.csv and extract bucket_id mapping
4. merge_bucket_ids() - Merge bucket_id into main dataframe before DAG generation
"""

import pandas as pd
from pathlib import Path

try:
    from config import get_bucket_paths
except ImportError:
    # Fallback for standalone usage
    def get_bucket_paths(env='dev'):
        project_root = Path(__file__).resolve().parent.parent
        buckets_dir = project_root / 'input' / 'buckets'
        buckets_dir.mkdir(parents=True, exist_ok=True)
        return {
            'bucket_input_csv': buckets_dir / f'{env}-bucket_input.csv',
            'final_bucket_info_csv': buckets_dir / 'FinalBucketInfo.csv',  # No env prefix
            'bucket_id_csv': buckets_dir / f'{env}-bucket_id.csv',
            'env_dir': buckets_dir
        }


def create_bucket_row(row, env='dev'):
    """Generate a single bucket configuration row from dataframe row.
    
    Args:
        row: Pandas Series with table configuration
        env: Environment ('dev' or 'prod')
        
    Returns:
        dict: Bucket configuration row
    """
    # Get table_name (derived column with banner prefix, e.g., mm_bp_data)
    table_name = row.get('table_name', row.get('dlTableName', ''))
    
    # Get schema name
    schema_name = row.get('dlSchemaName', '')
    
    # Get OP-Company code from ingestion.csv (already uppercase, e.g., SA-MM)
    op_cmpny_cd = row.get('OP-Company code', '').strip()
    
    # Fallback to constructing from banner if OP-Company code is missing
    if not op_cmpny_cd:
        banner_name = row.get('BANNER_NAME', row.get('bannerName', ''))
        op_cmpny_cd = f"SA-{banner_name.upper()}" if banner_name else "SA-"
    
    # Get sensitivity level from dataframe (hs, se, ns)
    sensitivity = str(row.get('dataSensitivity', 'ns')).strip().lower()
    
    
    # Generate service account based on sensitivity and environment
    service_account = f"svc-dl-sa-afaas-{sensitivity}@wmt-intl-dl-sa-{sensitivity}-{env}.iam.gserviceaccount.com"
    
    # Get load type and format for refresh mode
    load_type = str(row.get('tableLoadType', 'inc')).strip().upper()
    
    # Map load type to refresh mode
    if load_type == 'INC':
        refresh_mode = 'incremental load'
    elif load_type == 'FULL':
        refresh_mode = 'full load'
    else:
        refresh_mode = f"{load_type.lower()} load"
    
    return {
        'bucketNameType': 'hash',
        'databaseName': schema_name,
        'tableName': table_name,
        'opCmpnyCd': op_cmpny_cd,
        'refreshMode': refresh_mode,
        'wmt.storage_uploader': service_account,
        'wmt.storage_viewer': service_account,
        'isDevBigLake': 'FALSE',
        'updateSoftDelete': 'DCA Logic',
        'resourceBucketType': ''
    }


def generate_bucket_input_csv(df, output_file=None, env='dev'):
    """Generate bucket_input.csv from dataframe.
    
    This CSV is provided to the portal for bucket creation.
    
    Args:
        df: Pandas DataFrame with table configurations
        output_file: Path to save bucket_input.csv (default: from config)
        env: Environment ('dev' or 'prod', default 'prod')
        
    Returns:
        str: Path to the generated bucket_input.csv file
    """
    if output_file is None:
        bucket_paths = get_bucket_paths(env)
        output_file = bucket_paths['bucket_input_csv']
    
    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    bucket_rows = []
    for _, row in df.iterrows():
        try:
            bucket_row = create_bucket_row(row, env=env)
            bucket_rows.append(bucket_row)
        except Exception as e:
            table_name = row.get('icdsTableName', 'unknown')
            print(f"  ⚠️  Error creating bucket row for {table_name}: {e}")
    
    if bucket_rows:
        bucket_df = pd.DataFrame(bucket_rows)
        bucket_df.to_csv(output_file, index=False)
        
        # Summary statistics
        sensitivity_counts = {}
        for row in bucket_rows:
            sa = row['wmt.storage_uploader']
            # Extract sensitivity from service account
            if 'afaas-hs@' in sa:
                sensitivity_counts['hs'] = sensitivity_counts.get('hs', 0) + 1
            elif 'afaas-se@' in sa:
                sensitivity_counts['se'] = sensitivity_counts.get('se', 0) + 1
            elif 'afaas-ns@' in sa:
                sensitivity_counts['ns'] = sensitivity_counts.get('ns', 0) + 1
        
        print(f"\n Generated bucket_input.csv with {len(bucket_rows)} tables")
        print(f"   Location: {output_file}")
        print(f"   Environment: {env}")
        print(f"   Sensitivity breakdown:")
        for sens, count in sorted(sensitivity_counts.items()):
            print(f"     - {sens.upper()}: {count} table(s)")
        print(f"\n Next Step (Manual): Use this file to create buckets in the portal")
        bucket_paths = get_bucket_paths(env)
        print(f"   Save the portal result as: {bucket_paths['final_bucket_info_csv']}")
        return str(output_file)
    else:
        print(" No bucket rows generated")
        return None


def parse_final_bucket_info(final_bucket_file=None, env='dev'):
    """Parse FinalBucketInfo.csv and extract bucket_id mapping.
    
    This function reads the FinalBucketInfo.csv file that was returned
    from the bucket creation portal and extracts the bucket_name as bucket_id.
    
    Args:
        final_bucket_file: Path to FinalBucketInfo.csv (default: from config)
        env: Environment ('dev' or 'prod', default 'prod')
        
    Returns:
        pd.DataFrame: DataFrame with columns [tableName, bucket_id]
        
    Raises:
        FileNotFoundError: If FinalBucketInfo.csv is not found
    """
    if final_bucket_file is None:
        bucket_paths = get_bucket_paths(env)
        final_bucket_file = bucket_paths['final_bucket_info_csv']
    
    final_bucket_file = Path(final_bucket_file)
    
    if not final_bucket_file.exists():
        bucket_paths = get_bucket_paths(env)
        raise FileNotFoundError(
            f"\n FinalBucketInfo.csv not found at {final_bucket_file}\n"
            f"   Please create buckets using {bucket_paths['bucket_input_csv']} and save the result here."
        )
    
    # Read the FinalBucketInfo.csv file
    final_df = pd.read_csv(final_bucket_file)
    # Check for required columns
    required_columns = ['tableName', 'bucket_name']
    missing_cols = [col for col in required_columns if col not in final_df.columns]
    if missing_cols:
        raise ValueError(f"FinalBucketInfo.csv is missing columns: {missing_cols}. Found columns: {list(final_df.columns)}")
    # Extract required columns and rename
    bucket_mapping = final_df[['tableName', 'bucket_name']].copy()
    bucket_mapping.columns = ['tableName', 'bucket_id']
    # Convert to lowercase for consistent matching
    bucket_mapping['tableName'] = bucket_mapping['tableName'].str.lower()
    # Remove duplicates
    bucket_mapping = bucket_mapping.drop_duplicates(subset=['tableName'])
    print(f"\n Parsed FinalBucketInfo.csv")
    print(f"   Found {len(bucket_mapping)} bucket mappings")
    return bucket_mapping


def merge_bucket_ids(df, bucket_mapping_df):
    """Merge bucket_id into main dataframe.
    
    Args:
        df: Main dataframe with table_name column
        bucket_mapping_df: DataFrame from parse_final_bucket_info() with [tableName, bucket_id]
        
    Returns:
        pd.DataFrame: Dataframe with bucket_id column added
    """
    # Merge on lowercase full table name
    df_copy = df.copy()
    df_copy['_temp_table_lower'] = df_copy['table_name'].str.lower()
    bucket_mapping_df_copy = bucket_mapping_df.copy()
    bucket_mapping_df_copy['tableName'] = bucket_mapping_df_copy['tableName'].str.lower()
    # Merge using full table name
    df_merged = df_copy.merge(
        bucket_mapping_df_copy,
        left_on='_temp_table_lower',
        right_on='tableName',
        how='left'
    )
    # Drop temporary columns
    df_merged = df_merged.drop(columns=['_temp_table_lower', 'tableName'], errors='ignore')
    # Report merge results
    bucket_count = df_merged['bucket_id'].notna().sum()
    missing_count = df_merged['bucket_id'].isna().sum()
    print(f"\n🔗 Merged bucket IDs into dataframe:")
    print(f"   ✓ Tables with bucket_id: {bucket_count}")
    if missing_count > 0:
        print(f"    Tables without bucket_id: {missing_count}")
        missing_tables = df_merged[df_merged['bucket_id'].isna()]['table_name'].tolist()
        print(f"   Missing tables: {', '.join(missing_tables[:5])}")
    return df_merged
