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

def generate_docs():
    """Generate dbt documentation"""
    try:
        subprocess.run(["dbt", "docs", "generate", "--target", "gold"], check=True)
        print("Documentation generated successfully!")
    except subprocess.CalledProcessError as e:
        print(f"Error generating documentation: {e}")
        sys.exit(1)

def serve_docs():
    """Serve dbt documentation"""
    try:
        subprocess.run(["dbt", "docs", "serve", "--target", "gold"], check=True)
        print("Documentation server started!")
    except subprocess.CalledProcessError as e:
        print(f"Error serving documentation: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Run dbt pipeline')
    parser.add_argument('-f', '--file', type=str, help='Input CSV file name')
    parser.add_argument('-g', '--generate-docs', action='store_true', help='Generate dbt documentation')
    parser.add_argument('-s', '--serve-docs', action='store_true', help='Serve dbt documentation')
    
    args = parser.parse_args()

    if args.generate_docs:
        generate_docs()
        return

    if args.serve_docs:
        serve_docs()
        return

    if not args.file:
        print("Please provide an input CSV file using the -f flag")
        sys.exit(1)

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
