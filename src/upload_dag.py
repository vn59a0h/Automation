import os

def generate_upload_dag_script(dag_path, dest_bucket):
    script = f"gcloud storage cp {dag_path} {dest_bucket}\n"
    return script

def create_upload_dag_sh(dag_files, dest_bucket, output_path):
    with open(output_path, 'w') as f:
        f.write("#!/bin/bash\n\n")
        for dag in dag_files:
            f.write(generate_upload_dag_script(dag, dest_bucket))
    return output_path

