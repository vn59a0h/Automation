import os
def prepare_dag_file(sample_dag_file, dag_config):
    """
    Generate a customized DAG file from template using string replacement
    
    Args:
        sample_dag_file: Path to template DAG file
        dag_config: Dictionary with configuration values
        output_file: Path to output file
    """
    
    output_dir = dag_config['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{dag_config['dag_name']}.py")
    
    with open(sample_dag_file, 'r') as file:
        lines = file.readlines()
    
    updated_lines = []
    for line in lines:
        new_line = line
        
        # Edit1: Replace SENSITIVITY
        if line.strip().startswith('SENSITIVITY='):
            new_line = f'SENSITIVITY="{dag_config["sensitivity"]}"\n'
        
        # Edit2: Replace CLUSTER_NAME
        elif line.strip().startswith('CLUSTER_NAME ='):
            new_line = f'CLUSTER_NAME = "{dag_config["cluster_name"]}"\n'
        
        # Edit3: Replace BANNER_NAME
        elif line.strip().startswith('BANNER_NAME='):
            new_line = f'BANNER_NAME="{dag_config["banner_name"]}"\n'
        
        # Edit4: Replace TABLE_NAME
        elif line.strip().startswith('TABLE_NAME='):
            new_line = f'TABLE_NAME="{dag_config["table_name"]}" #SAP table name\n'
        
        # Edit5: Replace TAGS
        elif line.strip().startswith('TAGS ='):
            tags_str = str(dag_config["tags"]).replace("'", '"')
            new_line = f'TAGS = {tags_str}\n'
        
        updated_lines.append(new_line)
    
    # Write updated content to new file
    with open(output_file, 'w') as file:
        file.writelines(updated_lines)
        
    return output_file

def prepare_sql_file(sample_sql_file, dag_config, dlSchemaName, dlTableName,bucket_id):
    """
    Generate a customized SQL file from template using string replacement
    
    Args:
        sample_sql_file: Path to template SQL file
        dag_config: Dictionary with configuration values
        dlSchemaName: Schema name
        dlTableName: Table name
    """
    
    output_dir = dag_config['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, f"{dag_config['table_name']}.sql")
    
    with open(sample_sql_file, 'r') as file:
        content = file.read()
    
    # Replace placeholders with actual bucket path
    bucket_path = f"gs://{bucket_id}/{dag_config['table_name']}"
    
    updated_content = content.replace(
        'sa_mdse_dl_secure.mak_physl_invt_doc',
        f"{dlSchemaName}.{dag_config['table_name']}"
    )
    updated_content = updated_content.replace(
        "gs://86009702bc2c3bcd2c1c64cfb8e9a2e7ed7046d5e4b77786956ff1fe50586d/mak_physl_invt_doc",
        bucket_path
    )
    
    with open(output_file, 'w') as file:
        file.write(updated_content)
    
    print(f"✓ Created SQL file: {output_file}")
    return output_file

def generate_upload_commands(source_dir="../output", dest_bucket="gs://bfdaf-dags-intldlsadev-catalog/", output_file="../upload_commands.sh"):
    """
    Generate gcloud storage cp commands for all DAG files
    
    Args:
        source_dir: Directory containing generated DAG files
        dest_bucket: GCS destination bucket path
        output_file: Path to save the commands script
        
    Returns:
        list: List of gcloud commands
    """
    
    try:
        import glob
        
        commands = []
        
        # Find all DAG files
        dag_pattern = os.path.join(source_dir, "**", "INTLDLDAT-*.py")
        dag_files = sorted(glob.glob(dag_pattern, recursive=True))
        
        if not dag_files:
            print(f"No DAG files found in {source_dir}")
            return commands
        
        print(f"Found {len(dag_files)} DAG files")
        print("=" * 80)
        
        # Generate commands
        for dag_file in dag_files:
            cmd = f"gcloud storage cp {dag_file} {dest_bucket}"
            commands.append(cmd)
            print(cmd)
        
        print("=" * 80)
        
        # Save to file
        with open(output_file, 'w') as f:
            f.write("#!/bin/bash\n")
            f.write("# Auto-generated upload commands for DAG files\n\n")
            for cmd in commands:
                f.write(cmd + "\n")
        
        print(f"\n✓ Commands saved to: {output_file}")
        print(f"✓ Total commands: {len(commands)}")
        
        return commands
        
    except Exception as e:
        print(f"Error generating commands: {e}")
        raise

