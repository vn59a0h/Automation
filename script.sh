#!/bin/bash
set -x  # Enable debug mode to print each command before execution
chmod +x script.sh

# Step 0: Get user inputs
read -p "Enter SAP Table name: " SAP_TABLE
read -p "Enter Data Sensitivity (SE/NS/HS): " data_sensitivity
data_sensitivity=$(echo "$data_sensitivity" | tr '[:lower:]' '[:upper:]')
read -p "Enter Data Lake Schema name: " data_lake_schema
data_lake_schema=$(echo "$data_lake_schema" | tr '[:upper:]' '[:lower:]')
read -p "Enter Data Lake Table name: " data_lake_table
data_lake_table=$(echo "$data_lake_table" | tr '[:upper:]' '[:lower:]')
read -p "Enter Load Type (INC/FULL/HIST): " load_type
load_type=$(echo "$load_type" | tr '[:lower:]' '[:upper:]')

# Validate load type
while [[ ! "$load_type" =~ ^(INC|FULL|HIST)$ ]]; do
    echo "Invalid load type. Please enter INC, FULL, or HIST"
    read -p "Enter Load Type (INC/FULL/HIST): " load_type
    load_type=$(echo "$load_type" | tr '[:lower:]' '[:upper:]')
done

# Ask for banner selection
read -p "Do you want to process all banners? (yes/no): " all_banners

if [ "$all_banners" = "yes" ]; then
    banners=("mdd" "mak" "msb")
else
    banners=()
    read -p "Include MDD? (yes/no): " mdd
    [ "$mdd" = "yes" ] && banners+=("mdd")
    read -p "Include MAK? (yes/no): " mak
    [ "$mak" = "yes" ] && banners+=("mak")
    read -p "Include MSB? (yes/no): " msb
    [ "$msb" = "yes" ] && banners+=("msb")
fi

# Print all inputs
echo "SAP Table: $SAP_TABLE"
echo "Data Sensitivity: $data_sensitivity"
echo "Data Lake Schema: $data_lake_schema"
echo "Data Lake Table: $data_lake_table"
echo "Load Type: $load_type"
echo "Banners to process: ${banners[*]}"
echo "-----------------------------------"

# Create base directory structure
base_dir="./${SAP_TABLE}"
mkdir -p "$base_dir"

# Create docs directory
docs_dir="${base_dir}/${SAP_TABLE}_docs"
mkdir -p "$docs_dir"

# Create docs files
touch "${docs_dir}/Builders_${SAP_TABLE}.txt"
touch "${docs_dir}/Builders-${SAP_TABLE}_TABLE_SCHEMA.SQL"
touch "${docs_dir}/Game_${SAP_TABLE}.txt"
touch "${docs_dir}/GAME-${SAP_TABLE}_TABLE_SCHEMA.sql"
touch "${docs_dir}/Makro_${SAP_TABLE}.txt"
touch "${docs_dir}/MAKRO-${SAP_TABLE}_TABLE_SCHEMA.sql"
touch "${docs_dir}/${SAP_TABLE}_bucket_info.txt"
touch "${docs_dir}/${SAP_TABLE}_column_mapping.json"

# Create schema directory
schema_dir="${base_dir}/${data_lake_schema}"
mkdir -p "$schema_dir"

# Validate data sensitivity
while [[ ! "$data_sensitivity" =~ ^(SE|NS|HS)$ ]]; do
    echo "Invalid data sensitivity. Please enter SE, NS, or HS"
    read -p "Enter Data Sensitivity (SE/NS/HS): " data_sensitivity
    data_sensitivity=$(echo "$data_sensitivity" | tr '[:lower:]' '[:upper:]')
done

# Modify the generate_sql_content function
generate_sql_content() {
    local banner=$1
    local schema=$2
    local table=$3

    cat << EOF
CREATE EXTERNAL TABLE \`${schema}.${banner}_${table}\`(
  \`_hoodie_commit_time\` string COMMENT 'Hudi Meta Column',
  \`_hoodie_commit_seqno\` string COMMENT 'Hudi Meta Column',
  \`_hoodie_record_key\` string COMMENT 'Hudi Meta Column',
  \`_hoodie_partition_path\` string COMMENT 'Hudi Meta Column',
  \`_hoodie_file_name\` string COMMENT 'Hudi Meta Column',

  -- Primary Key Fields
  -- TODO: Add your primary key fields here
  
  -- Business Fields
  -- TODO: Add your business fields here

  -- DS Load Time
  \`ds_load_ts\` string COMMENT 'DS_LOAD_START_TS | timestamp'
)
 
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
   'hoodie.query.as.ro.table'='false',
   'path'='gs://bucket-id/${banner}_${table}')
STORED AS INPUTFORMAT
   'org.apache.hudi.hadoop.HoodieParquetInputFormat'
OUTPUTFORMAT
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
   'gs://bucket-id/${banner}_${table}';
EOF
}

# Create banner-specific directories and files
for banner in "${banners[@]}"; do
    # Convert banner to lowercase for directory naming
    banner_lower=$(echo "$banner" | tr '[:upper:]' '[:lower:]')
    banner_upper=$(echo "$banner" | tr '[:lower:]' '[:upper:]')
    schema_upper=$(echo "$data_lake_schema" | tr '[:lower:]' '[:upper:]')
    table_upper=$(echo "$data_lake_table" | tr '[:lower:]' '[:upper:]')
    
    # Create directory with lowercase naming
    banner_table_dir="${schema_dir}/${banner_lower}_${data_lake_table}"
    mkdir -p "$banner_table_dir"
    
    # Create Python file with correct DAG naming convention
    python_file="${banner_table_dir}/INTLDLDAT-SA${banner_upper}-${load_type}-${schema_upper}-${banner_upper}_${table_upper}.py"
    sql_file="${banner_table_dir}/${banner_lower}_${data_lake_table}.sql"
    
    # Create the files with proper documentation
    cat > "$python_file" << EOF
"""
DAG File: ${python_file##*/}
Purpose: Data ingestion pipeline for ${SAP_TABLE}

Naming Convention:
INTL - International
DL   - Data Lake
DAT  - Data
SA   - South Africa
${banner_upper} - Banner (MAK/MDD/MSB)
${load_type} - Load Type (INC/FULL/HIST)
${schema_upper} - Schema name (${data_sensitivity} sensitivity)
${banner_upper}_${table_upper} - Table name with banner prefix

Created by: Script
"""
EOF

    # Generate SQL content and write to file
    generate_sql_content "$banner_lower" "$data_lake_schema" "$data_lake_table" > "$sql_file"
done

# Print created structure for verification (only once)
echo "Directory structure created:"
tree "$base_dir"
# Print created structure for verification
echo "Directory structure created:"
tree "$base_dir"
    # Generate SQL content and write to file
    generate_sql_content "$banner_lower" "$data_lake_schema" "$data_lake_table" > "$sql_file"


# Print created structure for verification
echo "Directory structure created:"
tree "$base_dir"
# Print created structure for verification
echo "Directory structure created:"
tree "$base_dir"
tree "$base_dir"
# Print created structure for verification
echo "Directory structure created:"
tree "$base_dir"
