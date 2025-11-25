#!/usr/bin/env python3
"""
SE15 File Parser
Parses SAP SE15 table definition files and creates a single JSON file with all table metadata.

This version is intentionally simple:
- No NLP
- No "smart" renaming of columns
- Column names are taken directly from the SE15 file and never changed.
"""

import os
import json
import re
from pathlib import Path
from typing import List, Dict, Tuple

# Optional: Abbreviator can be used later, but this parser does NOT rename columns.
try:
    from abbreviator import Abbreviator
    ABBREV_AVAILABLE = True
except ImportError:
    try:
        from src.abbreviator import Abbreviator
        ABBREV_AVAILABLE = True
    except ImportError:
        ABBREV_AVAILABLE = False
        print("⚠️  Abbreviator module not available. Skipping abbreviation generation...")


def clean_table_name(table_name: str) -> str:
    """
    Clean table name by removing leading forward slashes and replacing
    internal forward slashes with underscores.

    Examples:
        "/SCDL/DB_PROCI_I" -> "SCDL_DB_PROCI_I"
        "/SCWM/AQUA"       -> "SCWM_AQUA"
        "BUT000"           -> "BUT000"
    """
    if not table_name:
        return table_name

    # Remove leading forward slash(es)
    cleaned = table_name.lstrip('/')

    # Replace remaining forward slashes with underscores
    cleaned = cleaned.replace('/', '_')

    return cleaned


import re
from typing import Dict, Tuple, List

def parse_se15_file(file_path: str) -> Tuple[Dict, Dict[str, any]]:
    """
    Parse a single SE15 .txt file and extract table and column metadata.

    This version is tailored to SE15 exports like:

        [header]    Table Name       Short Description
        [header]    Table field  Ac  Short Description  ...  Data Type  Length ...
        [row]       /SCDL/DB_DATE    Date
        [row]   X   MANDT       A    Client            ...  CLNT       3   ...

    - Uses the "Table Name" header row to find the table line.
    - Uses the "Table field" header row to find column positions.
    - Lines with 'X' in column 1 are column rows.
    - No NLP, no renaming; column_name is exactly as in the file.
    """
    try:
        with open(file_path, "r", encoding="utf-8-sig") as f:
            lines = f.readlines()

        if not lines:
            return None, {"error": "File is empty"}

        # ---------------------- 1. Find table header row ---------------------- #
        table_header_idx = None
        t_name_idx = None
        t_desc_idx = None

        for i, line in enumerate(lines):
            parts = line.rstrip("\r\n").split("\t")
            stripped = [p.strip() for p in parts]

            if "Table Name" in stripped and "Short Description" in stripped:
                table_header_idx = i
                t_name_idx = stripped.index("Table Name")
                t_desc_idx = stripped.index("Short Description")
                break

        if table_header_idx is None:
            return None, {"error": "Could not find 'Table Name' header row"}

        # ---------------------- 2. Find table data row ------------------------ #
        table_name = None
        table_description = None

        for i in range(table_header_idx + 1, len(lines)):
            parts = lines[i].rstrip("\r\n").split("\t")
            # Need at least up to t_name_idx and t_desc_idx
            if len(parts) <= max(t_name_idx, t_desc_idx):
                continue

            name_candidate = parts[t_name_idx].strip()
            desc_candidate = parts[t_desc_idx].strip() if len(parts) > t_desc_idx else ""

            if not name_candidate:
                continue

            # Skip if this line is just another header / junk
            if name_candidate in ("Table Name", "Table field"):
                continue

            table_name = name_candidate
            table_description = desc_candidate
            table_line_idx = i
            break

        if not table_name:
            return None, {"error": "Could not find table name in file"}

        # Clean table name like your original helper
        original_table_name = table_name
        table_name = clean_table_name(table_name)

        # ---------------------- 3. Find column header row --------------------- #
        col_header_idx = None
        idx_field_name = None
        idx_short_desc = None
        idx_key = None
        idx_data_type = None
        idx_length = None

        for i, line in enumerate(lines):
            parts = line.rstrip("\r\n").split("\t")
            stripped = [p.strip() for p in parts]

            if "Table field" in stripped and "Short Description" in stripped:
                col_header_idx = i
                idx_field_name = stripped.index("Table field")
                idx_short_desc = stripped.index("Short Description")
                # Optional fields (defensive)
                if "Key" in stripped:
                    idx_key = stripped.index("Key")
                if "Data Type" in stripped:
                    idx_data_type = stripped.index("Data Type")
                if "Length" in stripped:
                    idx_length = stripped.index("Length")
                break

        if col_header_idx is None:
            return None, {"error": "Could not find 'Table field' header row"}

        # ---------------------- 4. Parse column rows -------------------------- #
        columns: List[Dict[str, any]] = []

        # Column rows start after the column header row
        for line in lines[col_header_idx + 1:]:
            parts = line.rstrip("\r\n").split("\t")
            if not parts:
                continue

            # Your sample: column rows look like:
            #   ""  "X"  "MANDT"  "A"  "Client"  ...
            # So we require 'X' in column 1.
            if len(parts) < 2 or parts[1].strip() != "X":
                continue

            # Need field name
            if idx_field_name is None or idx_field_name >= len(parts):
                continue

            column_name = parts[idx_field_name].strip()
            if not column_name:
                continue

            # IMPORTANT: never change the column_name (no mapping, no NLP)
            column_description = (
                parts[idx_short_desc].strip()
                if idx_short_desc is not None and idx_short_desc < len(parts)
                else ""
            )

            is_primary_key = False
            if idx_key is not None and idx_key < len(parts):
                is_primary_key = (parts[idx_key].strip() == "X")

            data_type = ""
            if idx_data_type is not None and idx_data_type < len(parts):
                data_type = parts[idx_data_type].strip()

            length = ""
            if idx_length is not None and idx_length < len(parts):
                length = parts[idx_length].strip()

            columns.append(
                {
                    "column_name": column_name,     # EXACTLY from file
                    "column_abbrev": "",            # can be filled later
                    "sensitivity_level": "",        # can be filled later
                    "description": column_description,
                    "is_primary_key": is_primary_key,
                    "data_type": data_type,
                    "length": length,
                }
            )

        if not columns:
            return None, {"error": f"No columns found for table {table_name}"}

        table = {
            "table_name": table_name,           # cleaned (slashes -> underscores)
            "table_abbrev": "",                 # to be filled later
            "sensitivity_level": "",            # to be filled later
            "table_description": table_description,
            "columns": columns,
        }

        metadata = {
            "column_count": len(columns),
            "table_name": table_name,
        }

        return table, metadata

    except UnicodeDecodeError as e:
        return None, {"error": f"File encoding error: {str(e)}"}
    except Exception as e:
        return None, {"error": f"Unexpected error: {str(e)}"}



def parse_all_se15_files(directory: str) -> Dict[str, any]:
    """
    Parse all SE15 .txt files in the specified directory.

    This function ONLY parses.
    It does NOT rename column names or apply NLP.
    If you want abbreviations, do it in a separate step using Abbreviator
    and write into 'table_abbrev' / 'column_abbrev'.
    """
    results = {
        'tables': [],
        'total_files': 0,
        'processed_tables': 0,
        'total_columns': 0,
        'errors': []
    }

    # Check if directory exists
    if not os.path.exists(directory):
        results['errors'].append(f"Directory not found: {directory}")
        return results

    # Get all .txt files from directory
    txt_files = [f for f in os.listdir(directory) if f.endswith('.txt')]

    if not txt_files:
        results['errors'].append(f"No .txt files found in {directory}")
        return results

    results['total_files'] = len(txt_files)

    for filename in sorted(txt_files):
        file_path = os.path.join(directory, filename)

        print(f"Processing: {filename}")

        table_dict, metadata = parse_se15_file(file_path)

        if table_dict:
            results['tables'].append(table_dict)
            results['processed_tables'] += 1
            results['total_columns'] += metadata['column_count']
            print(f"  ✓ Parsed {metadata['table_name']}: {metadata['column_count']} columns")
        else:
            error_msg = f"{filename}: {metadata.get('error', 'Unknown error')}"
            results['errors'].append(error_msg)
            print(f"  ✗ Error: {metadata.get('error', 'Unknown error')}")

    return results


def save_to_json(tables_data: List[Dict], output_file: str = 'output/se15_tables.json'):
    """
    Save all table data to a single JSON file.
    
    Args:
        tables_data: List of dictionaries containing table and column metadata
        output_file: Path to the output JSON file
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_file).parent
    output_path.mkdir(exist_ok=True)
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(tables_data, f, indent=2, ensure_ascii=False)
        print(f"  ✓ Saved: {output_file}")
        return True
    except Exception as e:
        print(f"  ✗ Error saving {output_file}: {e}")
        return False


def print_summary(results: Dict[str, any], saved: bool):
    """
    Print a summary report of the parsing process.
    
    Args:
        results: Dictionary with processing results
        saved: Whether JSON file was saved successfully
    """
    print("\n" + "="*60)
    print("SUMMARY REPORT")
    print("="*60)
    print(f"Total files found:        {results['total_files']}")
    print(f"Tables parsed:            {results['processed_tables']}")
    print(f"Total columns extracted:  {results['total_columns']}")
    print(f"JSON file saved:          {'Yes' if saved else 'No'}")
    
    if results['errors']:
        print(f"\nErrors/Skipped ({len(results['errors'])} files):")
        for error in results['errors']:
            print(f"  - {error}")
    else:
        print("\n✓ All files processed successfully!")
    
    print("="*60)


class DataSensitivityClassifier:
    """Classify data sensitivity based on column descriptions and names."""
    
    def __init__(self):
        """Initialize the classifier with sensitivity keywords."""
        
        # Highly Sensitive (hs) - PII, Financial, Security data
        self.highly_sensitive_keywords = {
            # Personal Identifiable Information (strict)
            'password', 'passwd', 'pwd', 'secret', 'token', 'api key', 'private key',
            'credit card', 'card number', 'cvv', 'ssn', 'social security',
            'tax id', 'passport', 'driver license', 'national id',
            'bank account', 'account number', 'routing number', 'iban', 'swift',
            'salary', 'wage', 'compensation',
            'email address', 'phone number', 'mobile number', 'telephone',
            'birth date', 'date of birth', 'dob',
            'medical', 'health', 'diagnosis', 'prescription', 'patient',
            'biometric', 'fingerprint', 'facial', 'retina',
            'authentication', 'credential', 'pin', 'security question',
            # Security (strict only)
            'encrypted', 'hash', 'cipher'
        }
        
        # Sensitive (se) - Business data that needs evaluation (moved from hs)
        self.sensitive_keywords = {
            # Business operations (not highly sensitive but needs evaluation)
            'payment', 'transaction', 'invoice', 'billing',
            'pricing', 'cost', 'amount', 'value', 'price',
            'income', 'revenue',
            'address', 'street', 'residence', 'location',
            'age', 'key',
            # Delivery and warehouse operations
            'delivery', 'order', 'item', 'product', 'material',
            'warehouse', 'inventory', 'stock', 'quantity',
            'customer', 'client', 'partner', 'business partner'
        }
        
        # Non-Sensitive (ns) - General operational data
        self.non_sensitive_keywords = {
            'status', 'type', 'category', 'class', 'group',
            'code', 'id', 'identifier', 'number', 'sequence',
            'flag', 'indicator', 'marker', 'switch',
            'date', 'time', 'timestamp', 'created', 'modified', 'updated',
            'description', 'text', 'label', 'title', 'heading',
            'unit', 'count', 'total', 'sum',
            'version', 'release', 'revision',
            'language', 'locale', 'region', 'country code',
            'priority', 'level', 'rank', 'order',
            'dimension', 'measure', 'length', 'width', 'height', 'weight',
            'color', 'size', 'format', 'extension',
            'reference', 'link', 'url', 'path',
            'door', 'yard', 'staging', 'area', 'bin', 'wave'
        }
        
        # Exception patterns - even if they contain sensitive keywords, they're not sensitive
        self.exception_patterns = {
            'client', 'customer type', 'partner type', 'user type',
            'status code', 'error code', 'message type',
            'transaction type', 'payment type', 'document type'
        }
    
    def _contains_keyword(self, text: str, keywords: set) -> bool:
        """Check if text contains any keyword from the set."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        for keyword in keywords:
            # Use word boundaries to avoid partial matches
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, text_lower):
                return True
        
        return False
    
    def _is_exception(self, text: str) -> bool:
        """Check if text matches exception patterns."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        for pattern in self.exception_patterns:
            if pattern in text_lower:
                return True
        
        return False
    
    def classify_column(self, column_name: str, description: str, table_name: str = None) -> str:
        """
        Classify a column's sensitivity level.
        
        Priority:
        1. Check description first (most reliable)
        2. Check column name if description is empty or unclear
        
        Args:
            column_name: Name of the column
            description: Description of the column
            table_name: Optional table name for context
            
        Returns:
            'hs' - Highly Sensitive
            'ns' - Non-Sensitive
            'se' - Sensitive/Needs Evaluation (unclear)
        """
        # Combine description and column name for analysis
        # Prioritize description over column name
        primary_text = description if description else ""
        secondary_text = column_name if column_name else ""
        
        # Check for exceptions first
        if self._is_exception(primary_text) or self._is_exception(secondary_text):
            return 'ns'
        
        # Check description first (most reliable)
        if primary_text:
            # Check for highly sensitive first (strictest)
            if self._contains_keyword(primary_text, self.highly_sensitive_keywords):
                return 'hs'
            
            # Check for non-sensitive (most permissive)
            if self._contains_keyword(primary_text, self.non_sensitive_keywords):
                return 'ns'
            
            # Check for sensitive (middle ground)
            if self._contains_keyword(primary_text, self.sensitive_keywords):
                return 'se'
        
        # Check column name if description didn't give clear result
        if secondary_text:
            if self._contains_keyword(secondary_text, self.highly_sensitive_keywords):
                return 'hs'
            
            if self._contains_keyword(secondary_text, self.non_sensitive_keywords):
                return 'ns'
            
            if self._contains_keyword(secondary_text, self.sensitive_keywords):
                return 'se'
        
        # If no clear classification, mark as needs evaluation
        return 'se'
    
    def classify_table(self, table: Dict) -> str:
        """
        Classify table sensitivity based on column descriptions.
        Also adds sensitivity_level to each column.
        
        If ANY column is highly sensitive (hs), the entire table is hs.
        If all columns are non-sensitive (ns), the table is ns.
        Otherwise, the table is se (needs evaluation).
        
        Args:
            table: Table dictionary with columns
            
        Returns:
            Sensitivity level: 'hs', 'ns', or 'se'
        """
        table_name = table.get('table_name', '')
        has_highly_sensitive = False
        has_sensitive_eval = False
        all_non_sensitive = True
        
        # Analyze all columns
        for column in table.get('columns', []):
            column_name = column.get('column_name', '')
            description = column.get('description', '')
            
            sensitivity = self.classify_column(column_name, description, table_name)
            
            # Add sensitivity_level to column
            column['sensitivity_level'] = sensitivity
            
            if sensitivity == 'hs':
                has_highly_sensitive = True
                all_non_sensitive = False
            elif sensitivity == 'se':
                has_sensitive_eval = True
                all_non_sensitive = False
            elif sensitivity == 'ns':
                pass  # Non-sensitive column
        
        # Determine table-level sensitivity
        if has_highly_sensitive:
            return 'hs'
        elif all_non_sensitive:
            return 'ns'
        else:
            return 'se'


def main():
    """Main execution function."""
    print("SE15 File Parser with Abbreviation Generation & Sensitivity Classification")
    print("="*80)
    
    # Parse all files
    results = parse_all_se15_files('input/SE_15_Files')
    
    # Save to JSON (without abbreviations first)
    temp_file = 'output/se15_tables_temp.json'
    output_file = 'output/se15_tables.json'
    
    print(f"\nSaving {results['total_columns']} columns from {results['processed_tables']} tables...")
    save_to_json(results['tables'], temp_file)
    
    # Generate abbreviations if available
    if ABBREV_AVAILABLE:
        print("\nGenerating abbreviations...")
        try:
            abbr = Abbreviator('data/abbrev_data/word_abbreviations.json')
            
            # Process each table
            processed_count = 0
            for table in results['tables']:
                # Generate table abbreviation with context
                table_name = table.get('table_name', '')
                table_desc = table.get('table_description', '')
                table['table_abbrev'] = abbr.generate_abbreviation(
                    table_desc, 
                    context=table_name, 
                    is_table=True
                )
                
                # Generate column abbreviations
                for column in table.get('columns', []):
                    col_desc = column.get('description', '')
                    column['column_abbrev'] = abbr.generate_abbreviation(
                        col_desc,
                        context=None,
                        is_table=False
                    )
                
                processed_count += 1
            
            print(f"  ✓ Generated abbreviations for {processed_count} tables")
            
        except Exception as e:
            print(f"  ✗ Error generating abbreviations: {e}")
    else:
        print("  ⚠️  Skipping abbreviation generation...")
    
    # Classify data sensitivity
    print("\nClassifying data sensitivity...")
    try:
        classifier = DataSensitivityClassifier()
        
        sensitivity_stats = {'hs': 0, 'ns': 0, 'se': 0}
        
        for table in results['tables']:
            # Classify table and add sensitivity_level
            sensitivity = classifier.classify_table(table)
            table['sensitivity_level'] = sensitivity
            sensitivity_stats[sensitivity] += 1
        
        print(f"  ✓ Classified {results['processed_tables']} tables:")
        print(f"    - Highly Sensitive (hs): {sensitivity_stats['hs']}")
        print(f"    - Non-Sensitive (ns):    {sensitivity_stats['ns']}")
        print(f"    - Needs Evaluation (se): {sensitivity_stats['se']}")
        
    except Exception as e:
        print(f"  ✗ Error classifying sensitivity: {e}")
    
    # Save final version with abbreviations and sensitivity
    save_to_json(results['tables'], output_file)
    
    # Remove temp file
    Path(temp_file).unlink(missing_ok=True)
    
    # Print summary
    print_summary(results, True)


if __name__ == "__main__":
    main()
