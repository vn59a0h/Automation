import pandas as pd
from config import get_output_config_dir

def get_cluster_name(row):
    schema = row.dlSchemaName.lower().replace('_', '-')
    banner = row.BANNER_NAME.strip().lower()
    table = row.dlTableName.lower().replace('_', '-')
    return f"{schema}-{banner}-{table}"

def get_table_name(row):
    return f"{row.BANNER_NAME.strip().lower()}_{row.dlTableName.lower()}"

def get_dag_name(row):
    market = "SA"
    banner = row.BANNER_NAME.upper()
    load_type = row.tableLoadType.upper()
    schema_name = row.dlSchemaName.upper()
    table_name = row.table_name.upper()
    dag_name = f"INTLDLDAT-{market}{banner}-{load_type}-{schema_name}-{table_name}"
    return dag_name

def get_output_dir(row):
    schema_name = str(row.dlSchemaName).lower()
    table_name = str(row.table_name).lower()
    output_dir = str(get_output_config_dir(schema_name, table_name))
    return output_dir

def add_full_banner_name(row):
    if row.BANNER_NAME == 'MAK':
        full_banner_name = "makro"
    elif row.BANNER_NAME == 'MSB':
        full_banner_name = "builders"
    elif row.BANNER_NAME == 'MDD':
        full_banner_name = "game"
    elif row.BANNER_NAME == 'MM':
        full_banner_name = "massmart"
    else:
        full_banner_name = row.BANNER_NAME 
    return full_banner_name

def add_tags(row):
    market = 'SA'
    return ['Massmart-eComm', "P2", 'Ephemeral',f"{market}", 'SECURE', 'MDSE', f"{row.full_banner_name}", f"{row.table_name}", 'SLT']

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add all derived columns to the dataframe with explicit copy."""
    # Create explicit copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    df.loc[:, 'cluster_name'] = df.apply(get_cluster_name, axis=1)
    df.loc[:, 'table_name'] = df.apply(get_table_name, axis=1)
    df.loc[:, 'dag_name'] = df.apply(get_dag_name, axis=1)
    df.loc[:, 'output_dir'] = df.apply(get_output_dir, axis=1)
    df.loc[:, 'full_banner_name'] = df.apply(add_full_banner_name, axis=1)
    df.loc[:, 'tags'] = df.apply(add_tags, axis=1)
    
    return df
def check_and_remove_duplicates(df: pd.DataFrame, column: str, keep: str = 'first') -> tuple:
    """
    Check for duplicates in a dataframe column and remove them.
    
    Args:
        df: Input dataframe
        column: Column name to check for duplicates
        keep: Which duplicate to keep ('first', 'last', or False for all)
    
    Returns:
        Tuple of (cleaned_df, duplicate_records, removed_count, summary_dict)
    """
    # Get duplicates before removal
    duplicated_mask = df[column].duplicated(keep=False)
    duplicates = df[duplicated_mask].sort_values(column)
    
    # Get unique duplicate values
    duplicate_names = duplicates[column].unique()
    
    # Create summary
    summary = {
        'total_duplicate_records': len(duplicates),
        'unique_duplicate_values': len(duplicate_names),
        'duplicate_names': duplicate_names
    }
    
    # Print before removal
    print(f"\nTotal duplicate records: {len(duplicates)}")
    print(f"Unique duplicate values: {len(duplicate_names)}")
    
    if len(duplicates) > 0:
        
        print("\n" + "=" * 70)
        print(f"DUPLICATE {column.upper()}:")
        print("=" * 70)
        for i, name in enumerate(duplicate_names, 1):
            count = len(df[df[column] == name])
            print(f"{i}. {name} (appears {count} times)")
    else:
        print(f"\n✓ No duplicates found in '{column}'!")
        return df, duplicates, 0, summary
    
    # Remove duplicates
    print("\n" + "=" * 70)
    print(f"REMOVING DUPLICATE RECORDS (keeping '{keep}')")
    print("=" * 70)
    
    original_count = len(df)
    df_cleaned = df.drop_duplicates(subset=[column], keep=keep)
    removed_count = original_count - len(df_cleaned)
    
    print(f"Original records: {original_count}")
    print(f"Removed duplicates: {removed_count}")
    print(f"Final records: {len(df_cleaned)}")
    
    # Verify no duplicates remain
    remaining_duplicates = df_cleaned[column].duplicated().sum()
    print(f"\nRemaining duplicates: {remaining_duplicates}")
    if remaining_duplicates == 0:
        print(f"✓ All duplicates in '{column}' have been removed!")
    
    print("=" * 70)
    
    return df_cleaned, duplicates, removed_count, summary