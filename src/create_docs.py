import os
import json

def create_md_file(row, docs_dir):
    md_path = os.path.join(docs_dir, f"{row.table_name}.md")
    banner = row.get('full_banner_name') if (hasattr(row, 'get') and row.get('full_banner_name')) else row.get('BANNER_NAME', '')
    lines = [
        f"# {row.table_name}\n",
        f"- Schema: {row.dlSchemaName}\n",
        f"- Banner: {banner}\n"
    ]
    if row.get('dag_name'):
        lines.append(f"- DAG: {row.dag_name}.py\n")
    lines.append(f"- SQL: ./{row.table_name}.sql\n")
    with open(md_path, 'w') as f:
        f.writelines(lines)
    return md_path

def create_script_file(row, docs_dir):
    script_path = os.path.join(docs_dir, f"{row.table_name}.sh")
    script_lines = [
        "#!/bin/bash\n",
        f"# Script to run ingestion for {row.table_name}\n",
        f"echo \"Running job for {row.table_name}\"\n"
    ]
    with open(script_path, 'w') as f:
        f.writelines(script_lines)
    try:
        os.chmod(script_path, 0o755)
    except Exception:
        pass
    return script_path

def create_column_mapping_file(row, docs_dir):
    column_mapping_path = os.path.join(docs_dir, f"{row.table_name}.json")
    
    # Get column mapping
    mapping = None
    if hasattr(row, 'get'):
        mapping = row.get('column_mapping')
    else:
        mapping = getattr(row, 'column_mapping', None)
    if mapping is None:
        mapping = {}
    # Always add DS_LOAD_START_TS mapping
    mapping['DS_LOAD_START_TS'] = 'ds_load_ts'
    
    # Get bucket_id if available
    bucket_id = None
    if hasattr(row, 'get'):
        bucket_id = row.get('bucket_id')
    else:
        bucket_id = getattr(row, 'bucket_id', None)
    
    # Check if bucket_id is valid (not None, not NaN, not empty string)
    import pandas as pd
    if bucket_id is not None and not (isinstance(bucket_id, float) and pd.isna(bucket_id)) and bucket_id:
        target_bucket = f"gs://{bucket_id}/{row.table_name}/"
    else:
        target_bucket = ""
    
    # Extract primary key columns from column mapping or columns data
    primary_keys = []
    columns = row.get('columns') if hasattr(row, 'get') else getattr(row, 'columns', [])
    for col in columns:
        if col.get('is_primary_key', False):
            col_abbrev = col.get('column_abbrev', '')
            if col_abbrev:
                primary_keys.append(col_abbrev)
    
    # Normalize tableLoadType
    table_load_type = row.tableLoadType if hasattr(row, 'tableLoadType') else "incremental"
    if isinstance(table_load_type, str):
        if table_load_type.upper() == "INC":
            table_load_type = "incremental"
        elif table_load_type.upper() == "FULL":
            table_load_type = "full"
    
    # Build the complete JSON structure
    json_data = {
        "columnMapping": mapping,
        "dataSensitivity": row.dataSensitivity if hasattr(row, 'dataSensitivity') else "ns",
        "dlSchemaName": row.dlSchemaName if hasattr(row, 'dlSchemaName') else "",
        "dlTableName": row.table_name if hasattr(row, 'table_name') else "",
        "icdsTableName": f"EWM.dbo.{row.icdsTableName}_VW" if hasattr(row, 'icdsTableName') else "",
        "keyPreCombine": "ds_load_ts",
        "keyPrimaryKey": ", ".join(primary_keys) if primary_keys else "",
        "tableLoadType": table_load_type,
        "targetCatalogBucket": target_bucket
    }
    
    with open(column_mapping_path, 'w') as f:
        json.dump(json_data, f, indent=2)
    return column_mapping_path

def prepare_docs(row):
    output_dir = row.output_dir
    icds = row.get('icdsTableName') if hasattr(row, 'get') else getattr(row, 'icdsTableName', None)
    docs_dir_name = f"docs-{icds}" if icds else 'docs'
    docs_dir = os.path.join(output_dir, docs_dir_name)
    os.makedirs(docs_dir, exist_ok=True)

    md_path = create_md_file(row, docs_dir)
    script_path = create_script_file(row, docs_dir)
    column_mapping_path = create_column_mapping_file(row, docs_dir)

    return {
        # 'md': md_path,
        # 'script': script_path,
        'column_mapping': column_mapping_path
    }