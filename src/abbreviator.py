#!/usr/bin/env python3
"""
Abbreviation Generator for SE15 Tables and Columns
Now uses simple token + mapping logic (no stopwords, no NLP).

Logic:
  - Tokenize using [A-Za-z0-9]+
  - For each token:
        if token.lower() in word_to_abbr -> mapped abbreviation
        else                             -> token.lower()
  - Join with "_" and tidy multiple underscores
"""

import json
import re
import ssl
from pathlib import Path
from typing import Dict, List

class Abbreviator:
    """Generate abbreviated names from descriptions using simple token mapping."""
    
    WORD_RE = re.compile(r"[A-Za-z0-9]+")

    def __init__(self, abbreviations_file: str = 'data/abbrev_data/word_abbreviations.json'):
        """
        Initialize the abbreviator.
        
        Args:
            abbreviations_file: Path to word abbreviations JSON file
        """
        self.abbreviations_file = abbreviations_file
        self.word_to_abbr = self._load_abbreviations(abbreviations_file)
        # Debug flag to print missing words (similar to your do_column_mapping)
        self.debug_missing_words = False

    def _generate_abbreviations_file(self, csv_file: str, output_file: str) -> Dict[str, str]:
        """Generate word_abbreviations.json from CSV file."""
        import csv
        
        print(f" Generating {output_file} from {csv_file}...")
        
        word_abbr_map = {}
        
        # Try different encodings
        encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(csv_file, 'r', encoding=encoding, newline='', errors='ignore') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        word = row.get('Word', '').strip().lower()
                        abbr = row.get('Abbreviation', '').strip().lower()
                        
                        if word and abbr:
                            word_abbr_map[word] = abbr
                
                # Ensure output directory exists
                Path(output_file).parent.mkdir(parents=True, exist_ok=True)
                
                # Save JSON file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(word_abbr_map, f, indent=2, ensure_ascii=False)
                
                print(f"✓ Generated {len(word_abbr_map)} word abbreviations (encoding: {encoding})")
                return word_abbr_map
                
            except FileNotFoundError:
                print(f"  CSV file not found: {csv_file}")
                print("   Proceeding with empty abbreviation dictionary...")
                return {}
            except Exception as e:
                if encoding == encodings[-1]:  # Last encoding attempt
                    print(f"  Error reading CSV with all encodings: {e}")
                    print("   Proceeding with empty abbreviation dictionary...")
                    return {}
                continue  # Try next encoding
        
        return {}
        
    def _load_abbreviations(self, file_path: str) -> Dict[str, str]:
        """Load word abbreviation mappings and normalize like your mapping_dict."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except FileNotFoundError:
            print(f"  Abbreviations file not found: {file_path}")
            
            # Try to generate from CSV
            csv_file = str(Path(file_path).parent / 'datalake-naming-standards.csv')
            if Path(csv_file).exists():
                print(f"   Found CSV source: {csv_file}")
                data = self._generate_abbreviations_file(csv_file, file_path)
            else:
                print("   Using empty abbreviation dictionary...")
                data = {}

        # Normalize mapping like in your do_column_mapping (lowercase & stripped)
        norm: Dict[str, str] = {}
        if isinstance(data, dict):
            for k, v in data.items():
                if k is None or v is None:
                    continue
                key = str(k).strip().lower()
                val = str(v).strip().lower()
                if key:
                    norm[key] = val
        else:
            print("  Warning: abbreviations JSON is not a dict; using empty mapping.")
        return norm


    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text using [A-Za-z0-9]+ regex, like your code."""
        return self.WORD_RE.findall(text or "")

    def _map_text_to_slug(self, text: str, key: str = None) -> str:
        """
        Map a text description to slug using word_to_abbr.

        - Tokenize with WORD_RE
        - If token.lower() in mapping -> mapped abbreviation
        - Else -> token.lower()
        - Join with "_", collapse repeats, strip edges
        - Optionally print missing words
        """
        text = "" if text is None else str(text).strip()
        if not text:
            return ""

        tokens = self._tokenize(text)
        tokens_out: List[str] = []
        missing_words: List[str] = []

        for tok in tokens:
            lw = tok.lower()
            if lw in self.word_to_abbr:
                tokens_out.append(self.word_to_abbr[lw])
            else:
                tokens_out.append(lw)       # keep original lowercased
                missing_words.append(tok)   # for message

        slug = "_".join(tokens_out)
        slug = re.sub(r"_+", "_", slug).strip("_")

        if self.debug_missing_words and missing_words:
            for mw in missing_words:
                print(f'"{mw}" word not found in this line (key="{key or ""}", text="{text}")')

        return slug


    def generate_abbreviation(self, description: str, context: str = None, is_table: bool = False) -> str:
        """
        Generate abbreviated name from description.

        Flow kept same (description + context + is_table),
        but logic is now strictly token + mapping (your style).

        - First try mapping on description.
        - If result is empty and context is provided, fall back to context.
        - No stop words, no NLP, no smart truncation.
        """
        # First use description
        slug = self._map_text_to_slug(description, key=context)

        # Fallback: if nothing produced and context is available, use context
        if not slug and context:
            slug = self._map_text_to_slug(context, key=context)

        return slug


def add_abbreviations_to_json(input_json: str, output_json: str, 
                               abbreviations_file: str = 'abbrev_data/word_abbreviations.json'):
    """
    Add abbreviations to SE15 tables JSON.

    Uses the new mapping-only Abbreviator.
    
    Args:
        input_json: Input JSON file with tables and columns
        output_json: Output JSON file with abbreviations added
        abbreviations_file: Word abbreviations mapping file
    """
    # Initialize abbreviator
    abbr = Abbreviator(abbreviations_file)
    # If you want to see missing word messages, uncomment:
    # abbr.debug_missing_words = True
    
    # Load input JSON
    with open(input_json, 'r', encoding='utf-8') as f:
        tables = json.load(f)
    
    print(f"Processing {len(tables)} tables...")
    
    # Process each table
    for table in tables:
        table_name = table.get('table_name', '')
        table_desc = table.get('table_description', '')

        # TABLE abbreviation (same API, new logic)
        table['table_abbrev'] = abbr.generate_abbreviation(
            table_desc,
            context=table_name,
            is_table=True
        )
        
        # COLUMN abbreviations (same API, new logic)
        for column in table.get('columns', []):
            col_desc = column.get('description', '')
            col_name = column.get('column_name', '')  # adjust if key name is different
            column['column_abbrev'] = abbr.generate_abbreviation(
                col_desc,
                context=col_name,
                is_table=False
            )
    
    # Save output
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(tables, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Successfully processed {len(tables)} tables")
    print(f"✓ Output saved to: {output_json}")


if __name__ == "__main__":
    # Test the abbreviator
    input_file = 'output/se15_tables.json'
    output_file = 'output/se15_tables_with_abbrev.json'
    
    add_abbreviations_to_json(input_file, output_file)
