import os
from dotenv import load_dotenv
from pathlib import Path
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir) 

from csv_to_postgres import ingest_csv_to_postgres
import subprocess

# Load environment variables
parent_dir = Path(__file__).parent.parent
load_dotenv(parent_dir / '.env')

# Set default values
if not os.environ.get('DB_HOST'):
    os.environ['DB_HOST'] = os.getenv('DB_HOST', 'localhost')


ingest_csv_to_postgres(
    csv_path=f"{parent_dir}/Data Engineer Challenge_input.csv",
    schema="bronze",
    table_name="landing_egm_data",
    unique_key=["bus_date", "egm_description", "manufacturer", "venue_code", "fp"]
)
subprocess.run(["dbt", "build", "--target", "silver", "-s", "+tag:silver"])
subprocess.run(["dbt", "build", "--target", "gold", "-s", "+tag:gold", "--exclude-resource-types", "test"])
# subprocess.run(["dbt", "compile"])
