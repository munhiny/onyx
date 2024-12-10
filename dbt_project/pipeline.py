import os
from dotenv import load_dotenv
from pathlib import Path
import sys
import inspect
from ingest.csv_to_postgres import ingest_csv_to_postgres
import subprocess
import argparse


def run_pipeline(csv_name: str) -> None:
# Load environment variables
    parent_dir = Path(__file__).parent.parent
    load_dotenv(parent_dir / '.env')

    # Set default values
    if not os.environ.get('DB_HOST'):
        os.environ['DB_HOST'] = os.getenv('DB_HOST', 'localhost')


    ingest_csv_to_postgres(
        csv_path=f"{parent_dir}/{csv_name}",
        schema="bronze",
        table_name="landing_egm_data",
        unique_key=["bus_date", "egm_description", "manufacturer", "venue_code", "fp"]
    )
    subprocess.run(["dbt", "build", "--target", "silver", "-s", "+tag:silver"])
    subprocess.run(["dbt", "build", "--target", "gold", "-s", "+tag:gold", "--exclude-resource-types", "test"])
# subprocess.run(["dbt", "compile"])

def main():
    # Add argument parser
    parser = argparse.ArgumentParser(description='Process EGM data file')
    parser.add_argument('--file', '-f', 
                       default="Data Engineer Challenge_input.csv",
                       help='Name of the CSV file to process (should be in root directory)')
    
    args = parser.parse_args()
    
    # Load environment variables
    parent_dir = Path(__file__).parent.parent
    load_dotenv(parent_dir / '.env')

    # Set default values
    if not os.environ.get('DB_HOST'):
        os.environ['DB_HOST'] = os.getenv('DB_HOST', 'localhost')

    # Use the file argument
    csv_path = parent_dir / args.file

    ingest_csv_to_postgres(
        csv_path=str(csv_path),  # Convert Path to string
        schema="bronze",
        table_name="landing_egm_data",
        unique_key=["bus_date", "egm_description", "manufacturer", "venue_code", "fp"]
    )
    subprocess.run(["dbt", "build", "--target", "silver", "-s", "+tag:silver"])
    subprocess.run(["dbt", "build", "--target", "gold", "-s", "+tag:gold", "--exclude-resource-types", "test"])

if __name__ == "__main__":
    main()
