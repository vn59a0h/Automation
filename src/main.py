import argparse
import sys
import os
import pandas as pd
from pathlib import Path
from datetime import datetime

from utils import *
from dag_creation import *
from bucket import *


class IngestionPipeline:
    """Main orchestrator for the ingestion pipeline"""
    
    def __init__(self, ingestion_file, env='dev', dry_run=False):
        self.ingestion_file = ingestion_file
        self.env = env
        self.dry_run = dry_run
        self.log_file = f"ingestion_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.errors = []
        self.warnings = []
        
    def log(self, message, level="INFO"):
        """Print and log messages"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] [{level}] {message}"
        print(log_msg)
        with open(self.log_file, 'a') as f:
            f.write(log_msg + "\n")
    
    def validate_inputs(self):
        """Validate ingestion.csv before processing"""
        self.log("=" * 80)
        self.log("VALIDATION PHASE", "INFO")
        self.log("=" * 80)
        
        try:
            # Check file exists
            if not os.path.exists(self.ingestion_file):
                raise FileNotFoundError(f"File not found: {self.ingestion_file}")
            
            self.log(f"✓ Reading file: {self.ingestion_file}")
            df = pd.read_csv(self.ingestion_file)
            
            if df.empty:
                raise ValueError("ingestion.csv is empty")
            
            self.log(f"✓ Found {len(df)} rows to process")
            
            # Required columns
            required_cols = [
                'icdsTableName', 'BANNER_NAME', 'dataSensitivity', 
                'dlSchemaName', 'dlTableName', 'tableLoadType'
            ]
            
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            self.log(f"✓ All required columns present")
            
            # Validate data quality
            validation_issues = []
            
            for idx, row in df.iterrows():
                row_num = idx + 1
                
                # Check for empty critical fields
                if pd.isna(row['dlTableName']) or row['dlTableName'].strip() == '':
                    validation_issues.append(f"Row {row_num}: dlTableName is empty")
                
                if pd.isna(row['BANNER_NAME']) or row['BANNER_NAME'].strip() == '':
                    validation_issues.append(f"Row {row_num}: BANNER_NAME is empty")
                
                # Validate banner names
                banners = str(row['BANNER_NAME']).split(',')
                invalid_banners = [b.strip() for b in banners if b.strip() not in ['MDD', 'MAK', 'MSB']]
                if invalid_banners:
                    validation_issues.append(f"Row {row_num}: Invalid banner(s): {invalid_banners}")
                
                # Validate sensitivity
                valid_sensitivity = ['se', 'ns', 'hs']
                if str(row['dataSensitivity']).lower() not in valid_sensitivity:
                    validation_issues.append(f"Row {row_num}: Invalid sensitivity '{row['dataSensitivity']}'. Must be one of: {valid_sensitivity}")
                
                # Validate load type
                valid_load_types = ['INC', 'FULL']
                if str(row['tableLoadType']).upper() not in valid_load_types:
                    validation_issues.append(f"Row {row_num}: Invalid tableLoadType '{row['tableLoadType']}'. Must be INC or FULL")
            
            if validation_issues:
                for issue in validation_issues:
                    self.errors.append(issue)
                    self.log(f"✗ {issue}", "ERROR")
                return False
            
            self.log(f"✓ All {len(df)} rows validated successfully")
            return True
            
        except Exception as e:
            self.log(f"✗ Validation failed: {e}", "ERROR")
            self.errors.append(str(e))
            return False
    
    def check_dependencies(self):
        """Check if required files exist"""
        self.log("=" * 80)
        self.log("CHECKING DEPENDENCIES", "INFO")
        self.log("=" * 80)
        
        required_files = [
            ("../buckets/bucket_id.csv", "Bucket ID mapping"),
            ("../sample_dag.py", "Sample DAG template"),
            ("../sample_sql.sql", "Sample SQL template"),
        ]
        
        all_exist = True
        for filepath, description in required_files:
            if os.path.exists(filepath):
                self.log(f"✓ {description}: {filepath}")
            else:
                self.log(f"✗ Missing {description}: {filepath}", "ERROR")
                self.errors.append(f"Missing: {filepath}")
                all_exist = False
        
        return all_exist
    
    def run(self):
        """Execute the full pipeline"""
        
        # Phase 1: Validation
        if not self.validate_inputs():
            self.log("Pipeline aborted due to validation errors", "ERROR")
            self.print_summary()
            return False
        
        # Phase 2: Check dependencies
        if not self.check_dependencies():
            self.log("Pipeline aborted: missing dependencies", "ERROR")
            self.print_summary()
            return False
        
        # Phase 3: Extract bucket mappings
        self.log("=" * 80)
        self.log("EXTRACTING BUCKET MAPPINGS", "INFO")
        self.log("=" * 80)
        
        try:
            extract_bucket_id()
            self.log("✓ Bucket mappings extracted")
        except Exception as e:
            self.log(f"✗ Failed to extract bucket mappings: {e}", "ERROR")
            self.errors.append(str(e))
            self.print_summary()
            return False
        
        # Phase 4: Generate DAGs and SQL
        self.log("=" * 80)
        self.log("GENERATING DAGs AND SQL FILES", "INFO")
        self.log("=" * 80)
        
        try:
            df = pd.read_csv(self.ingestion_file)
            dag_list = self._prepare_dag_configuration(df)
            self.log(f"✓ Generated {len(dag_list)} DAG(s)")
        except Exception as e:
            self.log(f"✗ Failed to generate DAGs: {e}", "ERROR")
            self.errors.append(str(e))
            self.print_summary()
            return False
        
        # Phase 5: Generate upload commands
        self.log("=" * 80)
        self.log("GENERATING UPLOAD COMMANDS", "INFO")
        self.log("=" * 80)
        
        try:
            upload_commands = generate_upload_commands(
                source_dir="../output",
                dest_bucket=f"gs://bfdaf-dags-intldlsa{self.env}-catalog/",
                output_file="../upload_commands.sh"
            )
            self.log(f"✓ Generated {len(upload_commands)} upload command(s)")
        except Exception as e:
            self.log(f"✗ Failed to generate upload commands: {e}", "ERROR")
            self.errors.append(str(e))
            self.print_summary()
            return False
        
        self.print_summary()
        return True
    
    def _prepare_dag_configuration(self, df):
        """Generate DAG configuration for each table+banner combination"""
        dag_list = []
        
        for index, row in df.iterrows():
            tableLoadType = row['tableLoadType']
            dlSchemaName = row['dlSchemaName']
            dlTableName = row['dlTableName']
            dataSensitivity = row['dataSensitivity']
            banner_list = [b.strip() for b in str(row['BANNER_NAME']).split(',')]
            
            table_names = prepare_table_name(banner_list, dlTableName)
            cluster_names = prepare_cluster_name(dlSchemaName, banner_list, dlTableName)
            
            sample_dag_file = "../sample_dag.py"
            sample_sql_file = "../sample_sql.sql"
            
            for i, b in enumerate(banner_list):
                try:
                    dag_config = {
                        "sensitivity": dataSensitivity,
                        "cluster_name": cluster_names[i],
                        "banner_name": b,
                        "table_name": table_names[i],
                        "tags": ["Massmart-eComm","P2","Ephemeral","SA","SECURE","MDSE",f"{b}",f"{table_names[i]}","SLT"],
                        "tableLoadType": tableLoadType,
                        "output_dir": f"../output/{dlSchemaName}/{table_names[i]}",
                        "dag_name": f"INTLDLDAT-SA{b}-{tableLoadType}-{dlSchemaName.upper()}-{b}_{dlTableName}",
                    }
                    
                    create_bucket(dag_config, dlSchemaName, output_file="../buckets/bucket_input.csv", env=self.env)
                    bucket_id = get_bucket_id(dag_config)
                    prepare_dag_file(sample_dag_file, dag_config)
                    prepare_sql_file(sample_sql_file, dag_config, dlSchemaName, dlTableName, bucket_id)
                    
                    dag_list.append(dag_config)
                    self.log(f"✓ Generated DAG: {dag_config['dag_name']}")
                    
                except Exception as e:
                    self.log(f"✗ Error processing {dlTableName}/{b}: {e}", "ERROR")
                    self.errors.append(f"{dlTableName}/{b}: {str(e)}")
        
        return dag_list
    
    def print_summary(self):
        """Print execution summary"""
        self.log("=" * 80)
        self.log("EXECUTION SUMMARY", "INFO")
        self.log("=" * 80)
        self.log(f"Log file: {self.log_file}")
        self.log(f"Environment: {self.env}")
        self.log(f"Errors: {len(self.errors)}")
        self.log(f"Warnings: {len(self.warnings)}")
        
        if self.errors:
            self.log("\nErrors encountered:", "ERROR")
            for error in self.errors:
                self.log(f"  - {error}", "ERROR")
        
        if self.warnings:
            self.log("\nWarnings:", "WARNING")
            for warning in self.warnings:
                self.log(f"  - {warning}", "WARNING")
        
        self.log("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description='SAP Data Ingestion Automation Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run with default settings (dev environment)
  python main.py
  
  # Run for production
  python main.py --env prod
  
  # Dry-run to validate without generating files
  python main.py --dry-run
  
  # Use custom ingestion file
  python main.py --input /path/to/custom_ingestion.csv
        '''
    )
    
    parser.add_argument(
        '--input', '-i',
        default='../ingestion.csv',
        help='Path to ingestion.csv file (default: ../ingestion.csv)'
    )
    
    parser.add_argument(
        '--env', '-e',
        choices=['dev', 'test', 'prod'],
        default='dev',
        help='Target environment (default: dev)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate inputs without generating files'
    )
    
    args = parser.parse_args()
    
    pipeline = IngestionPipeline(
        ingestion_file=args.input,
        env=args.env,
        dry_run=args.dry_run
    )
    
    success = pipeline.run()
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
