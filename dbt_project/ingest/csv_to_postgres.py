import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

def ingest_csv_to_postgres(
    csv_path: str,
    table_name: str,
    schema: str,
    unique_key: str | list[str],
    updated_at_column: str = 'updated_at'
) -> None:
    """
    Incrementally load CSV data into PostgreSQL
    
    Args:
        csv_path: Path to the CSV file
        table_name: Name of the table in PostgreSQL
        schema: Schema name in PostgreSQL
        unique_key: Column name(s) that uniquely identify each record. Can be a single column name or list of column names
        updated_at_column: Column name for tracking last update timestamp
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Add or update timestamp column
        df[updated_at_column] = datetime.now()
        
        # Create PostgreSQL connection
        db_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        engine = create_engine(
            f"postgresql://{db_params['user']}:{db_params['password']}@"
            f"{db_params['host']}:{db_params['port']}/{db_params['database']}"
        )

        # Convert unique_key to list if it's a string
        unique_keys = [unique_key] if isinstance(unique_key, str) else unique_key
        
        with engine.connect() as connection:
            # Check if schema exists and create if it doesn't
            schema_exists = connection.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.schemata WHERE schema_name = '{schema}')"
            )).scalar()

            if not schema_exists:
                connection.execute(text(f"CREATE SCHEMA {schema}"))
                connection.commit()
                print(f"Created new schema {schema}")

            # Check if table exists
            table_exists = connection.execute(text(
                f"SELECT EXISTS (SELECT FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}')"
            )).scalar()

            if not table_exists:
                # If table doesn't exist, create it with initial data
                df.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='replace',
                    index=False
                )
                print(f"Created new table {schema}.{table_name} with {len(df)} records")
                return

            # Create a merge key for comparison
            def create_merge_key(df):
                return df[unique_keys].astype(str).agg('|'.join, axis=1)

            existing_df = pd.read_sql(f"SELECT * FROM {schema}.{table_name}", engine)
            
            existing_merge_key = create_merge_key(existing_df)
            new_merge_key = create_merge_key(df)

            # Split data into records to insert and update
            new_records = df[~new_merge_key.isin(existing_merge_key)]
            potential_updates = df[new_merge_key.isin(existing_merge_key)]

            # Filter updates to only include records where monitored columns have changed
            monitored_columns = ["turnover_sum", "gmp_sum", "games_played_sum"]
            
            # Create merge dictionary for existing records
            existing_dict = {
                create_merge_key(existing_df.iloc[[i]]).iloc[0]: i 
                for i in range(len(existing_df))
            }
            # Filter updates where monitored columns have changed
            updates = pd.DataFrame()
            # Iterate through each row in the DataFrame of potential updates
            for idx, row in potential_updates.iterrows():
                # Create a unique identifier (merge key) for the current row
                # Using iloc[[idx]] to maintain DataFrame structure, then get first element with iloc[0]
                merge_key = create_merge_key(potential_updates.iloc[[idx]]).iloc[0]
                
                # Look up the corresponding index in the existing data using the merge key
                existing_idx = existing_dict[merge_key]
                
                # Get the matching row from the existing DataFrame using the found index
                existing_row = existing_df.iloc[existing_idx]
                
                # Compare values between new and existing rows for specified columns
                # If any monitored column has a different value, add row to updates
                if any(row[col] != existing_row[col] for col in monitored_columns):
                    # Add the row with changes to the updates DataFrame using concat

                    updates = pd.concat([updates, row.to_frame().T])

            # Insert new records
            if not new_records.empty:
                new_records.to_sql(
                    name=table_name,
                    schema=schema,
                    con=engine,
                    if_exists='append',
                    index=False
                )
                print(f"Inserted {len(new_records)} new records")

            # Update existing records
            if not updates.empty:
                # Create temporary table for updates
                temp_table = f"temp_{table_name}"
                updates.to_sql(temp_table, schema=schema, con=engine, if_exists='replace', index=False)

                # Modified update query to handle multiple join conditions
                update_cols = [col for col in df.columns if col not in unique_keys]
                set_statements = [f"{col} = source.{col}" for col in update_cols]
                join_conditions = [f"target.{key} = source.{key}" for key in unique_keys]
                
                update_query = f"""
                    UPDATE {schema}.{table_name} AS target
                    SET {', '.join(set_statements)}
                    FROM {schema}.{temp_table} AS source
                    WHERE {' AND '.join(join_conditions)}
                """
                connection.execute(text(update_query))
                
                # Drop temporary table
                connection.execute(text(f"DROP TABLE {schema}.{temp_table}"))
                print(f"Updated {len(updates)} existing records")

        print(f"Incremental load completed successfully for {schema}.{table_name}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
