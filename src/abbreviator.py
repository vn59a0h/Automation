#!/usr/bin/env python3
"""
Abbreviation Generator for SE15 Tables and Columns
Generates standardized abbreviated names based on descriptions.
"""

import json
import re
import ssl
from pathlib import Path
from typing import Dict, List

# Optional: Install NLTK for better results
try:
    import nltk
    from nltk.stem import WordNetLemmatizer
    from nltk.corpus import stopwords
    
    # Fix SSL certificate issue for NLTK downloads
    try:
        _create_unverified_https_context = ssl._create_unverified_context
    except AttributeError:
        pass
    else:
        ssl._create_default_https_context = _create_unverified_https_context
    
    # Download required data (first time only)
    resources = {
        'punkt': 'tokenizers/punkt',
        'punkt_tab': 'tokenizers/punkt_tab',
        'stopwords': 'corpora/stopwords',
        'wordnet': 'corpora/wordnet',
        'averaged_perceptron_tagger': 'taggers/averaged_perceptron_tagger',
        'averaged_perceptron_tagger_eng': 'taggers/averaged_perceptron_tagger_eng'
    }
    
    for resource, path in resources.items():
        try:
            nltk.data.find(path)
        except LookupError:
            print(f"Downloading {resource}...")
            nltk.download(resource, quiet=True)
    
    NLP_AVAILABLE = True
    lemmatizer = WordNetLemmatizer()
    stop_words = set(stopwords.words('english'))
except ImportError:
    NLP_AVAILABLE = False
    print("  NLTK not available. Using basic text processing...")
    print("   For better results: pip install nltk\n")


class Abbreviator:
    """Generate abbreviated names from descriptions."""
    
    def __init__(self, abbreviations_file: str = 'data/abbrev_data/word_abbreviations.json'):
        """
        Initialize the abbreviator.
        
        Args:
            abbreviations_file: Path to word abbreviations JSON file
        """
        self.abbreviations_file = abbreviations_file
        self.word_to_abbr = self._load_abbreviations(abbreviations_file)
        self.max_length = 20  # Reduced from 30 to 20 for more reasonable table names
        self.min_table_length = 6  # Minimum length for table abbreviations
        self.custom_stop_words = {'table'}  # Removed 'general' and 'data' to use them in abbreviations
    
    def _generate_abbreviations_file(self, csv_file: str, output_file: str):
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
        """Load word abbreviation mappings."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"  Abbreviations file not found: {file_path}")
            
            # Try to generate from CSV
            csv_file = str(Path(file_path).parent / 'datalake-naming-standards.csv')
            if Path(csv_file).exists():
                print(f"   Found CSV source: {csv_file}")
                return self._generate_abbreviations_file(csv_file, file_path)
            else:
                print("   Using fallback abbreviation strategy...")
                return {}
    
    def _extract_words(self, text: str, preserve_acronyms: bool = True) -> List[str]:
        """Extract important words from text using NLP or basic processing."""
        if not text:
            return []
        
        # Extract acronyms first (2+ consecutive uppercase letters)
        acronyms = []
        if preserve_acronyms:
            acronyms = re.findall(r'\b[A-Z]{2,}\b', text)
        
        if NLP_AVAILABLE:
            # NLP approach: lemmatize and filter by POS tags
            words = nltk.word_tokenize(text.lower())
            pos_tags = nltk.pos_tag(words)
            
            # Keep nouns and verbs, skip stop words
            important_words = []
            
            # Add acronyms first (lowercased)
            important_words.extend([a.lower() for a in acronyms])
            
            for word, pos in pos_tags:
                if not word.isalpha():
                    continue
                if word in stop_words or word in self.custom_stop_words:
                    continue
                if pos.startswith(('NN', 'VB')):  # Nouns and Verbs
                    lemma = lemmatizer.lemmatize(word, pos='n' if pos.startswith('NN') else 'v')
                    important_words.append(lemma)
            return important_words
        else:
            # Basic approach: simple tokenization
            words = re.findall(r'\b[a-zA-Z]+\b', text.lower())
            basic_stop_words = {
                'of', 'the', 'a', 'an', 'and', 'or', 'to', 'for', 'with', 
                'in', 'on', 'at', 'by', 'from', 'is', 'are', 'was', 'were'
            }
            all_stop = basic_stop_words | self.custom_stop_words
            
            # Add acronyms first
            result = [a.lower() for a in acronyms]
            result.extend([w for w in words if w not in all_stop and len(w) > 1 and w.lower() not in [a.lower() for a in acronyms]])
            return result
    
    def _abbreviate_word(self, word: str) -> str:
        """Get abbreviation for a word."""
        # Try dictionary lookup first
        if word in self.word_to_abbr:
            return self.word_to_abbr[word]
        
        # Fallback: truncate to 4 characters
        return word[:4]
    
    def _truncate_name(self, parts: List[str]) -> str:
        """
        Truncate name to fit max length using smart strategies.
        
        Strategy:
        1. If it fits, return as-is
        2. Limit to maximum 3 parts (keep first, middle, last)
        3. Remove shortest parts if still too long
        4. Truncate individual parts proportionally as last resort
        """
        name = '_'.join(parts)
        
        if len(name) <= self.max_length:
            return name
        
        # Strategy 1: Limit to maximum 3 parts (keeps most important words)
        if len(parts) > 3:
            # Keep first, middle, and last parts
            middle_idx = len(parts) // 2
            parts = [parts[0], parts[middle_idx], parts[-1]]
            name = '_'.join(parts)
            
            if len(name) <= self.max_length:
                return name
        
        # Strategy 2: Remove shortest parts first (but keep at least 2)
        while len('_'.join(parts)) > self.max_length and len(parts) > 2:
            # Find shortest part
            min_idx = min(range(len(parts)), key=lambda i: len(parts[i]))
            parts.pop(min_idx)
        
        name = '_'.join(parts)
        
        # Strategy 3: Truncate each part proportionally
        if len(name) > self.max_length:
            chars_per_part = max(3, (self.max_length - len(parts) + 1) // len(parts))
            parts = [p[:chars_per_part] for p in parts]
            name = '_'.join(parts)
        
        return name[:self.max_length]
    
    def generate_abbreviation(self, description: str, context: str = None, is_table: bool = False) -> str:
        """
        Generate abbreviated name from description.
        
        Strategy for tables:
        1. Generate abbreviation from key words in description
        2. If too short, try using ALL words from description
        3. If still too short, add relevant parts from table name
        
        Args:
            description: Text description (e.g., "Warehouse Task Status")
            context: Optional context like table name (e.g., "BUT000")
            is_table: Whether this is a table (True) or column (False)
            
        Returns:
            Abbreviated name (e.g., "whse_task_stat")
        """
        if not description:
            return ""
        
        # Step 1: Extract important words and generate initial abbreviation
        words = self._extract_words(description, preserve_acronyms=is_table)
        
        if not words:
            # No words found - use table name if available
            if is_table and context:
                clean_name = re.sub(r'[^a-z0-9]', '_', context.lower()).strip('_')
                clean_name = re.sub(r'_+', '_', clean_name)
                return clean_name[:self.max_length]
            # Fallback: use raw description
            clean = re.sub(r'[^a-z0-9]', '', description.lower())
            return clean[:self.max_length]
        
        # Abbreviate each word
        parts = [self._abbreviate_word(word) for word in words]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_parts = []
        for part in parts:
            if part not in seen:
                seen.add(part)
                unique_parts.append(part)
        
        # Generate initial abbreviation
        abbrev = self._truncate_name(unique_parts)
        
        # For columns, return the abbreviation as-is
        if not is_table:
            return abbrev
        
        # For tables: Check if we need enhancement
        is_too_short = len(abbrev) < self.min_table_length
        is_single_word = len(unique_parts) == 1
        
        if not (is_too_short or is_single_word):
            return abbrev  # Good enough!
        
        # Step 2: Try using ALL words from description (less filtering)
        all_text_words = re.findall(r'\b[a-zA-Z]+\b', description.lower())
        minimal_stop_words = {'the', 'a', 'an', 'and', 'or', 'to', 'for', 'with', 'in', 'on', 'at', 'by', 'from', 'is', 'are', 'was', 'were', 'of'}
        
        # Extract acronyms
        acronyms = re.findall(r'\b[A-Z]{2,}\b', description)
        
        # Build extended word list from description
        extended_words = [a.lower() for a in acronyms]
        extended_words.extend([w for w in all_text_words if w not in minimal_stop_words and w not in [a.lower() for a in acronyms]])
        
        # Abbreviate extended words
        extended_parts = [self._abbreviate_word(word) for word in extended_words]
        
        # Remove duplicates
        seen = set()
        extended_unique = []
        for part in extended_parts:
            if part not in seen:
                seen.add(part)
                extended_unique.append(part)
        
        # Generate extended abbreviation from description
        extended_abbrev = self._truncate_name(extended_unique)
        
        # Check if description-based abbreviation is now good enough
        if len(extended_abbrev) >= self.min_table_length and len(extended_unique) > 1:
            return extended_abbrev
        
        # Step 3: Description is too short, add table name parts
        if context:
            # Extract meaningful parts from table name
            table_parts = re.findall(r'[A-Z]+|[a-z]+|\d+', context)
            meaningful_table_parts = [p.lower() for p in table_parts if len(p) > 1]
            
            # Abbreviate table name parts
            table_abbrevs = [self._abbreviate_word(p) for p in meaningful_table_parts]
            
            # Smart combination: avoid redundancy
            # Check if any description abbreviation is similar to table parts
            combined_parts = list(extended_unique)  # Start with description
            
            for table_abbr in table_abbrevs:
                # Skip if this table abbreviation is already in description parts
                if table_abbr in extended_unique:
                    continue
                
                # Skip if table abbreviation is substring of any description part or vice versa
                is_redundant = False
                for desc_part in extended_unique:
                    if (table_abbr in desc_part or desc_part in table_abbr) and len(table_abbr) > 2:
                        is_redundant = True
                        break
                
                if not is_redundant:
                    combined_parts.append(table_abbr)
            
            combined_abbrev = self._truncate_name(combined_parts[:4])  # Limit to 4 parts
            
            return combined_abbrev
        
        # No table context available, return what we have
        return extended_abbrev


def add_abbreviations_to_json(input_json: str, output_json: str, 
                               abbreviations_file: str = 'abbrev_data/word_abbreviations.json'):
    """
    Add abbreviations to SE15 tables JSON.
    
    Args:
        input_json: Input JSON file with tables and columns
        output_json: Output JSON file with abbreviations added
        abbreviations_file: Word abbreviations mapping file
    """
    # Initialize abbreviator
    abbr = Abbreviator(abbreviations_file)
    
    # Load input JSON
    with open(input_json, 'r', encoding='utf-8') as f:
        tables = json.load(f)
    
    print(f"Processing {len(tables)} tables...")
    
    # Process each table
    for table in tables:
        # Generate table abbreviation
        table_desc = table.get('table_description', '')
        table['table_abbrev'] = abbr.generate_abbreviation(table_desc)
        
        # Generate column abbreviations
        for column in table.get('columns', []):
            col_desc = column.get('description', '')
            column['column_abbrev'] = abbr.generate_abbreviation(col_desc)
    
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
