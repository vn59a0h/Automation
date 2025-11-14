Project Architecture Overview
Your project is a SAP Data Ingestion Automation Pipeline that orchestrates the movement of data from SAP ICDS (source) to Google Cloud Storage (GCS) via Apache Airflow. Here's the complete flow:

SAP ICDS Table Metadata
    ↓
Parse & Create Bucket Request (bucket_input.csv)
    ↓
Portal Creates GCS Bucket → FinalBucketInfo.csv
    ↓
Extract Bucket IDs → bucket_id.csv
    ↓
Generate DAG + SQL files using bucket_id
    ↓
[MANUAL] Add CCM configurations
    ↓
Upload DAG to Airflow Catalog
    ↓
Airflow executes DAG → Data flows SAP → GCS

Key Components in Your Project
Component	File	Purpose
Input	ingestion.csv	Table metadata from SAP
Bucket Config Generator	bucket.py	Creates bucket requests
Bucket Mapping	bucket_id.csv	Maps tables to GCS bucket IDs
DAG Generator	dag_creation.py	Creates Python DAG files
Orchestrator	notebook.ipynb	Coordinates entire flow
CCM Integration	ccm.py	(TODO) Add CCM config generation
Deployment	upload_commands.sh	gcloud commands to upload DAGs

┌─────────────────────────────────────────────────────────────────┐
│                     INPUT PHASE                                  │
├─────────────────────────────────────────────────────────────────┤
│  ingestion.csv (table metadata)                                 │
│  ├─ SAP Table Names                                             │
│  ├─ Banner/Division (MDD, MAK, MSB)                             │
│  ├─ Schema Names (sa_mdse_dl_secure, sa_mdse_dl_table)          │
│  └─ Load Type (INC = Incremental)                               │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 1: BUCKET CREATION (Your Notebook)            │
├─────────────────────────────────────────────────────────────────┤
│  • Parse ingestion.csv for each SAP table                       │
│  • Generate bucket_input.csv with specs:                        │
│    - Database name, Table name, Banner                          │
│    - Service account permissions                                │
│    - Refresh mode (incremental)                                 │
│  • Upload to GCS Bucket Portal                                  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 2: BUCKET ID RETRIEVAL (Portal)               │
├─────────────────────────────────────────────────────────────────┤
│  Portal creates buckets and returns:                            │
│  FinalBucketInfo.csv                                            │
│  ├─ tableName → mdd_physl_invt_doc                              │
│  └─ bucket_name → gs://676e39d3ff0cc7e5...                      │
│                                                                  │
│  Your script processes this into: bucket_id.csv                 │
│  (Maps table_name to bucket_id for lookup)                      │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│           STEP 3: DAG & SQL GENERATION (Your Notebook)          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  For each banner+table combination:                             │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Generate DAG File (Python)                              │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ INTLDLDAT-SA{BANNER}-INC-{SCHEMA}-{TABLE}.py           │   │
│  │ Contains:                                               │   │
│  │ • Airflow DAG definition                                │   │
│  │ • Task orchestration (extract/load)                     │   │
│  │ • Metadata (tags, cluster_name, sensitivity)            │   │
│  │ • NOT YET: CCM configurations                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Generate SQL File                                       │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ {table_name}.sql                                        │   │
│  │ Contains:                                               │   │
│  │ • CREATE EXTERNAL TABLE statement                       │   │
│  │ • Hudi metadata columns                                 │   │
│  │ • Bucket path: gs://{bucket_id}/{table_name}            │   │
│  │ • LOCATION points to GCS bucket                         │   │
│  │ • Manual section for primary keys (TODO)                │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  Output structure:                                              │
│  output/                                                        │
│  └─ sa_mdse_dl_secure/                                          │
│     ├─ mdd_physl_invt_doc/                                      │
│     │  ├─ INTLDLDAT-SAMDD-INC-SA_MDSE_DL_SECURE-....py         │
│     │  └─ mdd_physl_invt_doc.sql                               │
│     ├─ mak_physl_invt_doc/                                      │
│     │  ├─ INTLDLDAT-SAMAK-INC-SA_MDSE_DL_SECURE-....py         │
│     │  └─ mak_physl_invt_doc.sql                               │
│     └─ msb_physl_invt_doc/                                      │
│        ├─ INTLDLDAT-SAMSB-INC-SA_MDSE_DL_SECURE-....py         │
│        └─ msb_physl_invt_doc.sql                               │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│           STEP 4: CCM PORTAL CONFIG (Manual + Your Task)        │
├─────────────────────────────────────────────────────────────────┤
│  • Visit CCM (Configuration Management) Portal                  │
│  • For each DAG, add:                                           │
│    - Connection configs                                         │
│    - Source/target system parameters                            │
│    - Data transformation rules                                  │
│    - Error handling policies                                    │
│  • This info is NOT in the generated DAG yet                    │
│    (This is where ccm.py would come in)                         │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│        STEP 5: DAG UPLOAD TO AIRFLOW (Your Notebook)            │
├─────────────────────────────────────────────────────────────────┤
│  • Generate gcloud commands to upload DAGs                      │
│  • Source: output/{schema}/{table}/INTLDLDAT-*.py              │
│  • Destination: gs://bfdaf-dags-intldlsadev-catalog/           │
│  • Commands saved to: upload_commands.sh                        │
│                                                                  │
│  Example:                                                       │
│  gcloud storage cp ../output/sa_mdse_dl_secure/                │
│    mdd_physl_invt_doc/INTLDLDAT-SAMDD-INC-....py \             │
│    gs://bfdaf-dags-intldlsadev-catalog/                         │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│              STEP 6: AIRFLOW EXECUTION (Production)             │
├─────────────────────────────────────────────────────────────────┤
│  • Airflow scheduler picks up DAG from catalog bucket           │
│  • DAG tasks execute:                                           │
│    1. Extract from SAP ICDS                                     │
│    2. Transform data                                            │
│    3. Load to GCS bucket (path from SQL file)                   │
│  • Hudi handles incremental updates                             │
│  • BigLake tables query the data                                │
└─────────────────────────────────────────────────────────────────┘