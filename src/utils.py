import pandas as pd

def read_conf_csv():
    """Read and print the content of ingestion.csv file."""
    try:
        csv_reader = pd.read_csv('ingestion.csv')
        print(csv_reader)
    except FileNotFoundError:
        print("Error: ingestion.csv file not found")
    except Exception as e:
        print(f"Error reading CSV file: {e}")

read_conf_csv()