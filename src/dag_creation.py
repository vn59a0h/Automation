def prepare_dag_file(sample_dag_file, dag_config, output_file):
    """
    Generate a customized DAG file from template using string replacement
    
    Args:
        sample_dag_file: Path to template DAG file
        dag_config: Dictionary with configuration values
        output_file: Path to output file
    """
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
    
    print(f"✓ Created: {output_file}")
    return output_file