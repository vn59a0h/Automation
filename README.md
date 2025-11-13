# DAG Automation Project

## Setup Instructions

1. Run the setup script:
```bash
chmod +x setup.sh
./setup.sh
```

2. Activate the virtual environment:
```bash
source venv/bin/activate
```

3. Launch Jupyter Notebook:
```bash
jupyter notebook
```

4. Open `src/notebook.ipynb` to start working

## Project Structure

```
Ingestion/Automation/
├── src/
│   ├── notebook.ipynb      # Main notebook for DAG generation
│   └── utils.py            # Utility functions
├── sample_dag.py           # Template DAG file
├── ingestion.csv           # Configuration CSV
├── requirements.txt        # Python dependencies
├── setup.sh               # Setup script
└── README.md              # This file
```

## Usage

The notebook will:
1. Read configuration from `ingestion.csv`
2. Generate DAG names based on banner, schema, and table info
3. Create customized DAG files from the template
4. Save generated DAGs to the output directory

## Deactivate Virtual Environment

```bash
deactivate
```
