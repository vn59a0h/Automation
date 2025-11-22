"""
Centralized configuration for file paths and directories.
All path references should use this config to ensure consistency.
"""
import os
from pathlib import Path

# Project root directory (use absolute path)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input directories
INPUT_DIR = PROJECT_ROOT / "input"
INPUT_CSV = INPUT_DIR / "ingestion.csv"
SE15_FILES_DIR = INPUT_DIR / "SE_15_Files"
BUCKETS_DIR = INPUT_DIR / "buckets"

# Environment-specific bucket paths (use get_bucket_paths() to get correct paths)
BUCKET_INPUT_CSV = BUCKETS_DIR / "bucket_input.csv"  # Deprecated - use get_bucket_paths()
BUCKET_ID_CSV = BUCKETS_DIR / "bucket_id.csv"  # Deprecated - use get_bucket_paths()
FINAL_BUCKET_INFO_CSV = BUCKETS_DIR / "FinalBucketInfo.csv"  # Deprecated - use get_bucket_paths()


def get_bucket_paths(env='dev'):
    """
    Get environment-specific bucket file paths with env prefix.
    
    Args:
        env: Environment ('dev' or 'prod', default 'dev')
        
    Returns:
        dict: Paths for bucket files in the specified environment
    """
    ensure_dir_exists(BUCKETS_DIR)
    
    return {
        'bucket_input_csv': BUCKETS_DIR / f'{env}-bucket_input.csv',
        'final_bucket_info_csv': BUCKETS_DIR / 'FinalBucketInfo.csv',  # No env prefix
        'bucket_id_csv': BUCKETS_DIR / f'{env}-bucket_id.csv',
        'env_dir': BUCKETS_DIR
    }

# Data directories (reference/template data)
DATA_DIR = PROJECT_ROOT / "data"
ABBREV_DATA_DIR = DATA_DIR / "abbrev_data"
SAMPLE_DATA_DIR = DATA_DIR / "sample_data"

# Abbreviation files
WORD_ABBREVIATIONS_JSON = ABBREV_DATA_DIR / "word_abbreviations.json"
DATALAKE_NAMING_CSV = ABBREV_DATA_DIR / "datalake-naming-standards.csv"

# Sample/template files
SAMPLE_DAG_PY = SAMPLE_DATA_DIR / "sample_dag.py"
SAMPLE_SQL = SAMPLE_DATA_DIR / "sample_sql.sql"

# Output directories
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_CONFIGS_DIR = OUTPUT_DIR / "configs"
SE15_TABLES_JSON = OUTPUT_DIR / "se15_tables.json"
SE15_TABLES_TEMP_JSON = OUTPUT_DIR / "se15_tables_temp.json"
SE15_TABLES_WITH_ABBREV_JSON = OUTPUT_DIR / "se15_tables_with_abbrev.json"
SE15_TABLES_WITH_SENSITIVITY_JSON = OUTPUT_DIR / "se15_tables_with_sensitivity.json"
SENSITIVITY_REPORT_TXT = OUTPUT_DIR / "sensitivity_report.txt"


def get_output_config_dir(schema_name: str, table_name: str) -> Path:
    """
    Get output directory path for a specific schema and table.
    
    Args:
        schema_name: Database schema name
        table_name: Table name
        
    Returns:
        Path object for the output directory
    """
    return OUTPUT_CONFIGS_DIR / schema_name.lower() / table_name.lower()


def ensure_dir_exists(path: Path) -> Path:
    """
    Ensure directory exists, create if it doesn't.
    
    Args:
        path: Directory path
        
    Returns:
        The same path object
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


# Ensure critical directories exist
ensure_dir_exists(OUTPUT_DIR)
ensure_dir_exists(OUTPUT_CONFIGS_DIR)
