import os
from config import SAMPLE_SQL, SAMPLE_DAG_PY


def prepare_sql_file(row):
    
    output_dir = row.output_dir
    os.makedirs(output_dir, exist_ok=True)

    output_sql_file = os.path.join(output_dir, f"{row.table_name}.sql")

    # Get columns data
    columns = row.get('columns') if hasattr(row, 'get') else getattr(row, 'columns', [])
    
    # Generate column definitions
    primary_key_columns = []
    business_columns = []
    
    for col in columns:
        col_name = col.get('column_name', '')
        col_abbrev = col.get('column_abbrev', '')
        col_desc = col.get('description', '')
        is_pk = col.get('is_primary_key', False)
        
        if not col_abbrev or not col_name:
            continue
            
        # Format: `column_abbrev` string COMMENT 'COLUMN_NAME | Description - PRIMARY KEY'
        comment = f"{col_name} | {col_desc}"
        if is_pk:
            comment += " - PRIMARY KEY"
            column_def = f"  `{col_abbrev}` string COMMENT '{comment}',"
            primary_key_columns.append(column_def)
        else:
            column_def = f"  `{col_abbrev}` string COMMENT '{comment}',"
            business_columns.append(column_def)
    
    # Build the SQL content
    bucket_id = row.get('bucket_id') if hasattr(row, 'get') else None
    bucket_path = f"gs://{bucket_id}/{row.table_name}" if bucket_id else "gs://nan/{row.table_name}"
    
    sql_content = f"""CREATE EXTERNAL TABLE `{row.dlSchemaName}.{row.table_name}`(
  -- Hudi Meta Columns
  `_hoodie_commit_time` string COMMENT 'Hudi Meta Column',
  `_hoodie_commit_seqno` string COMMENT 'Hudi Meta Column',
  `_hoodie_record_key` string COMMENT 'Hudi Meta Column',
  `_hoodie_partition_path` string COMMENT 'Hudi Meta Column',
  `_hoodie_file_name` string COMMENT 'Hudi Meta Column',
  
  -- Primary Key Fields
"""
    
    # Add primary key columns
    for pk_col in primary_key_columns:
        sql_content += pk_col + "\n"
    
    sql_content += "\n  -- Business Columns\n"
    
    # Add business columns
    for biz_col in business_columns:
        sql_content += biz_col + "\n"
    
    # Add final timestamp column (no trailing comma)
    sql_content += """
  `ds_load_ts` timestamp COMMENT 'DS_LOAD_START_TS | Data Load Timestamp'

)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
   'hoodie.query.as.ro.table'='false',
   'path'='""" + bucket_path + """')
STORED AS INPUTFORMAT
   'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   '""" + bucket_path + """'
"""

    # Write SQL file
    with open(output_sql_file, 'w') as f:
        f.write(sql_content)
        
    return output_sql_file

def prepare_dag_file(row):
    output_dir = row.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    output_dag_file = os.path.join(row.output_dir, f"{row.dag_name}.py")
    
    sample_dag_file = str(SAMPLE_DAG_PY)
   
    with open(sample_dag_file, 'r') as file:
        lines = file.readlines()

    updated_lines = []
    for line in lines:
        new_line = line

        # Edit1: Replace SENSITIVITY
        if line.strip().startswith('SENSITIVITY='):
            new_line = f'SENSITIVITY="{row.dataSensitivity.upper()}"\n'

        # Edit2: Replace CLUSTER_NAME
        elif line.strip().startswith('CLUSTER_NAME ='):
            new_line = f'CLUSTER_NAME = "{row.cluster_name}"\n'

        # Edit3: Replace BANNER_NAME
        elif line.strip().startswith('BANNER_NAME='):
            new_line = f'BANNER_NAME="{row.full_banner_name}"\n'

        # Edit4: Replace TABLE_NAME
        elif line.strip().startswith('TABLE_NAME='):
            new_line = f'TABLE_NAME="{row.icdsTableName}" #SAP table name\n'

        # Edit5: Replace TAGS
        elif line.strip().startswith('TAGS ='):
            tags_str = str(row.tags).replace("'", '"')
            new_line = f'TAGS = {tags_str}\n'

        updated_lines.append(new_line)

    # Write updated content to new file
    with open(output_dag_file, 'w') as file:
        file.writelines(updated_lines)
        
    return output_dag_file