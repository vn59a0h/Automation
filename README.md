# SAP Data Ingestion Automation Pipeline

Quick guide for ingesting SAP tables into GCS via Apache Airflow.

## Setup (One-time)

```bash
cd /Users/vn59a0h/Desktop/projects/Ingestion/Automation
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Quick Start for Team Members

### Step 1: Prepare Your Table List

Edit `ingestion.csv` and add your 10 tables:

| Column | Example | Notes |
|--------|---------|-------|
| icdsTableName | IKPF | SAP table name |
| BANNER_NAME | MDD,MAK,MSB | Banners to ingest |
| dataSensitivity | se | se/ns/hs |
| dlSchemaName | sa_mdse_dl_secure | Target schema |
| dlTableName | PHYSL_INVT_DOC | Lowercase table name |
| tableLoadType | INC | INC=Incremental, FULL=Full |

**Example:**
```csv
icdsTableName,BANNER_NAME,dataSensitivity,OP-Company code,dlSchemaName,dlTableName,tableLoadType,keyPreCombine,keyPrimaryKey,bucket_id
IKPF,"MDD,MAK,MSB",se,"SA-MDD,SA-MAK,SA-MSB",sa_mdse_dl_secure,PHYSL_INVT_DOC,INC,ds_load_ts,"clnt,application,cond_type",123
```

### Step 2: Validate Your Input

```bash
cd src
python main.py --dry-run
```

This will check for common mistakes:
- ✓ Missing columns
- ✓ Invalid banner names (must be MDD, MAK, or MSB)
- ✓ Invalid sensitivity (must be se, ns, or hs)
- ✓ Empty required fields

### Step 3: Generate DAGs and SQL

```bash
python main.py --env dev
```

This will:
1. ✓ Validate all inputs
2. ✓ Create bucket requests (bucket_input.csv)
3. ✓ Extract bucket IDs
4. ✓ Generate DAG Python files
5. ✓ Generate SQL files
6. ✓ Create upload commands

**Output:**
```
output/
├── sa_mdse_dl_secure/
│   ├── mdd_physl_invt_doc/
│   │   ├── INTLDLDAT-SAMDD-INC-SA_MDSE_DL_SECURE-MDD_PHYSL_INVT_DOC.py
│   │   └── mdd_physl_invt_doc.sql
│   ├── mak_physl_invt_doc/
│   └── msb_physl_invt_doc/
└── sa_mdse_dl_table/
    └── ...
```

### Step 4: Upload DAGs to Airflow

```bash
bash ../upload_commands.sh
```

Or manually upload one DAG:
```bash
gcloud storage cp output/sa_mdse_dl_secure/mdd_physl_invt_doc/INTLDLDAT-*.py \
  gs://bfdaf-dags-intldlsadev-catalog/
```

### Step 5: Add CCM Configuration (Manual)

1. Visit CCM Portal
2. For each generated DAG, add:
   - Connection configs
   - Source/target system parameters
   - Error handling policies

## Common Issues & Solutions

### ❌ "EmptyDataError: No columns to parse from file"
**Solution:** Make sure `bucket_id.csv` has data. If empty, contact lead to run `extract_bucket_id()` first.

### ❌ "Missing required columns"
**Solution:** Check your `ingestion.csv`. Required columns:
- icdsTableName
- BANNER_NAME
- dataSensitivity
- dlSchemaName
- dlTableName
- tableLoadType

### ❌ "Invalid banner(s): XYZ"
**Solution:** Banner names must be exactly: `MDD`, `MAK`, or `MSB` (case-sensitive)

### ❌ "Table 'xxx' not found in bucket_id.csv"
**Solution:** The table hasn't been allocated a bucket yet. Contact team lead.

### ❌ "File not found: ../sample_dag.py"
**Solution:** Make sure you're running from the `src/` directory. Run: `cd src && python main.py`

## File Descriptions

| File | Purpose |
|------|---------|
| `ingestion.csv` | Your input: list of SAP tables to ingest |
| `src/main.py` | CLI tool (run this!) |
| `src/bucket.py` | Bucket creation logic |
| `src/dag_creation.py` | DAG & SQL generation |
| `buckets/bucket_id.csv` | Maps table names to GCS bucket IDs |
| `output/` | Generated DAG and SQL files |
| `upload_commands.sh` | Auto-generated gcloud upload commands |

## Environment-Specific Commands

```bash
# Development environment
python main.py --env dev

# Test environment
python main.py --env test

# Production environment (requires approval!)
python main.py --env prod
```

## Troubleshooting: Check the Log File

Every run creates a log file: `ingestion_run_YYYYMMDD_HHMMSS.log`

```bash
# View recent log
cat ingestion_run_*.log | tail -50
```

## Need Help?

Contact your team lead with:
1. The error message from the log file
2. The row number in ingestion.csv causing the issue
3. Your ingestion.csv content
