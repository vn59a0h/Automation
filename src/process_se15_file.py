import pandas as pd
from pathlib import Path
from config import SE15_FILES_DIR

def check_se15_file_exists(icds_table_name: str, base_path: str = None) -> bool:
    """Check if SE15 file exists for the given ICDS table name."""
    if not icds_table_name or pd.isna(icds_table_name):
        return False
    
    # Use default path from config if not provided
    if base_path is None:
        base_path = SE15_FILES_DIR
    
    folder_path = Path(base_path)
    if not folder_path.exists():
        return False
    
    normalized_icds = str(icds_table_name).lower()
    
    try:
        for file in folder_path.iterdir():
            if file.is_file() and file.suffix == '.txt':
                normalized_se15 = file.stem.replace(' ', '_').lower()
                if normalized_se15 == normalized_icds:
                    return True
    except Exception as e:
        print(f"Error checking file for {icds_table_name}: {e}")
        return False
    
    return False

def map_se15_files_to_dataframe(df: pd.DataFrame, base_path: str = None) -> pd.DataFrame:
    """Add SE15 file status column to dataframe with explicit copy."""
    # Use default path from config if not provided
    if base_path is None:
        base_path = SE15_FILES_DIR
    
    # Create explicit copy to avoid SettingWithCopyWarning
    df = df.copy()
    df.loc[:, 'se15_file_exists'] = df['icdsTableName'].apply(
        lambda x: check_se15_file_exists(x, base_path)
    )
    return df

from pathlib import Path

def extract_short_description(file_path: Path) -> str | None:
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            
        # Prepare a normalized version of the filename for comparison
        # e.g. "SCDL DB_DATE" -> "scdldbdate"
        filename_clean = file_path.stem.replace(' ', '').replace('_', '').lower()

        for line in lines:
            # Split by tab and remove empty strings
            parts = [p.strip() for p in line.split('\t') if p.strip()]
            
            if not parts:
                continue
                
            # Skip the header line
            if parts[0].lower() == 'table name':
                continue

            # Strategy 1: Look for lines with exactly 2 parts (Table Name, Description)
            if len(parts) == 2:
                # Check if the first part looks like the table name (optional safety check)
                # We return the second part as the description
                return parts[1]
            
            # Strategy 2: If there are more parts, check if the first part matches the filename
            if len(parts) > 1:
                # Normalize the potential table name from the file content
                # e.g. "/SCDL/DB_DATE" -> "scdldbdate"
                content_table_clean = parts[0].replace('/', '').replace('_', '').lower()
                
                if content_table_clean == filename_clean:
                    return parts[1]

    except Exception as e:
        print(f"Failed parsing {file_path.name}: {e}")
    return None

def build_se15_short_desc_mapping(base_path: str) -> dict[str, str]:
    mapping = {}
    p = Path(base_path)
    if not p.exists():
        return mapping
    for txt in p.glob('*.txt'):
        # Normalize key: remove spaces, lowercase
        # This matches how we might look it up later
        norm = txt.stem.replace(' ', '_').lower()
        desc = extract_short_description(txt)
        if desc:
            mapping[norm] = desc
        else:
            print(f"Warning: Could not extract description for {txt.name}")
    return mapping
