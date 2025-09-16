import os
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import logging
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
import json
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from sqlalchemy import text
import streamlit as st

load_dotenv()

logger = logging.getLogger(__name__)

def fetch_dataframe(database_name, query, params=None):
    """
    Connects to a database via a specific SSH tunnel and fetches data using a SQL query.

    This function establishes an SSH tunnel to 'ebalina.com' to securely connect
    to the database. It is designed for read-only operations (SELECT statements)
    and returns the results as a pandas DataFrame.

    Args:
        database_name (str): The name of the database to connect to (e.g., 'pharmacircle').
        query (str): The SQL SELECT query to execute.
        params (dict, optional): A dictionary of parameters to bind to the query for safety.

    Returns:
        pandas.DataFrame: A DataFrame containing the query results, or None if an error occurs.
    """
    try:
        logger.info(f"Establishing SSH tunnel to connect to DB '{database_name}'...")

        # Get credentials from environment variables
        db_user = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        ssh_user = st.secrets["SSH_USER"]
        ssh_password = st.secrets["SSH_PASSWORD"]

        if not all([db_user, db_password, ssh_user, ssh_password]):
            raise ValueError("Required environment variables (DB_USER, DB_PASSWORD, SSH_USER, SSH_PASSWORD) are not set.")

        # Define SSH tunnel and remote database parameters
        ssh_host = st.secrets["SSH_HOST"]
        ssh_port = st.secrets["SSH_PORT"]
        remote_db_host = st.secrets["DB_HOST"]
        remote_db_port = st.secrets["DB_PORT"]

        with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            remote_bind_address=(remote_db_host, remote_db_port),
        ) as tunnel:
            logger.info("SSH tunnel established successfully. Connecting to the database.")
            
            # Create the connection URL pointing to the local end of the tunnel
            mysql_url = f"mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{database_name}"
            
            engine = create_engine(mysql_url)
            
            with engine.connect() as connection:
                logger.info("Database connection successful. Fetching data...")
                df = pd.read_sql(text(query), connection, params=params)
                logger.info(f"Successfully fetched {len(df)} rows.")
                return df

    except Exception as e:
        logger.error(f"Could not fetch data from '{database_name}'. Error: {e}")
        return None


def execute_query(database_name, query, params=None):
    """
    Connects to a database via a specific SSH tunnel and executes a command that modifies data.

    This function is for write operations (INSERT, UPDATE, DELETE). It runs
    the query within a transaction, which means the changes are only saved
    if the entire operation succeeds. It returns the number of rows affected.

    Args:
        database_name (str): The name of the database to connect to (e.g., 'llm').
        query (str): The SQL command to execute (e.g., INSERT, UPDATE, DELETE).
        params (dict, optional): A dictionary of parameters to safely bind to the query.

    Returns:
        int: The number of rows affected by the command, or -1 if an error occurs.
    """
    try:
        logger.info(f"Establishing SSH tunnel to connect to DB '{database_name}' for command execution...")

        # Get credentials from environment variables
        db_user = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        ssh_user = st.secrets["SSH_USER"]
        ssh_password = st.secrets["SSH_PASSWORD"]

        if not all([db_user, db_password, ssh_user, ssh_password]):
            raise ValueError("Required environment variables (DB_USER, DB_PASSWORD, SSH_USER, SSH_PASSWORD) are not set.")

        # Define SSH tunnel and remote database parameters
        ssh_host = st.secrets["SSH_HOST"]
        ssh_port = st.secrets["SSH_PORT"]
        remote_db_host = st.secrets["DB_HOST"]
        remote_db_port = st.secrets["DB_PORT"]
        
        with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            remote_bind_address=(remote_db_host, remote_db_port),
        ) as tunnel:
            logger.info("SSH tunnel established successfully. Connecting to the database.")
            
            mysql_url = f"mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{database_name}"
            engine = create_engine(mysql_url)
            
            with engine.connect() as connection:
                with connection.begin() as transaction:
                    try:
                        logger.info("Database connection successful. Executing query...")
                        result = connection.execute(text(query), params or {})
                        transaction.commit()
                        logger.info(f"Query executed successfully. {result.rowcount} rows affected.")
                        return result.rowcount
                    except Exception as e:
                        logger.error(f"Error during query execution: {e}. Rolling back transaction.")
                        transaction.rollback()
                        raise

    except Exception as e:
        logger.error(f"Failed to execute query on '{database_name}'. Error: {e}")
        return -1
    
def add_alternative_record(row_id, column_to_update, new_data):
    """
    Adds a new alternative row to the test_results table.

    Args:
        row_id (str): The base row ID for the alternative group.
        column_to_update (str): The database column name to populate with new data.
        new_data (any): The new raw data from the stream to be inserted.

    Returns:
        str: The ID of the newly created record, or None on failure.
    """
    try:
        # Fetch the original record (alt_id=0) to use as a template
        base_record_query = "SELECT * FROM `test_results` WHERE `row_id` = :row_id AND `alt_id` = 0 LIMIT 1"
        base_df = fetch_dataframe("llm", base_record_query, params={'row_id': row_id})

        if base_df is None or base_df.empty:
            logger.error(f"Could not find original record for row_id: {row_id}")
            return None

        new_record = base_df.iloc[0].to_dict()

        # Find the highest current alt_id to determine the new one
        max_alt_id_query = "SELECT MAX(alt_id) as max_id FROM `test_results` WHERE `row_id` = :row_id"
        max_alt_df = fetch_dataframe("llm", max_alt_id_query, params={'row_id': row_id})
        
        new_alt_id = 0
        if max_alt_df is not None and not max_alt_df.empty:
            max_id = max_alt_df['max_id'].iloc[0]
            if pd.notna(max_id):
                new_alt_id = int(max_id) + 1

        # Update the record with new alternative info
        new_record['alt_id'] = new_alt_id
        new_record['id'] = f"{row_id}-{new_alt_id}"
        updated_record = new_record.copy()
        updated_record['alt_id'] = new_alt_id
        updated_record['id'] = f"{row_id}-{new_alt_id}"
        updated_record[column_to_update] = new_data
        
        # Ensure all values are JSON serializable where necessary
        for key, value in updated_record.items():
            if isinstance(value, (dict, list)):
                updated_record[key] = json.dumps(value)

        # Build and execute the INSERT query
        columns = ", ".join([f"`{col}`" for col in updated_record.keys() if col in base_df.columns])
        placeholders = ", ".join([f":{col}" for col in updated_record.keys() if col in base_df.columns])
        
        insert_query = f"INSERT INTO `test_results` ({columns}) VALUES ({placeholders})"
        
        # Filter params to only include columns that exist in the table
        valid_params = {k: v for k, v in updated_record.items() if k in base_df.columns}

        execute_query("llm", insert_query, params=valid_params)
        logger.info(f"Successfully added alternative record with ID: {updated_record['id']}")
        return updated_record['id']

    except Exception as e:
        logger.error(f"Failed to add alternative record for row_id {row_id}. Error: {e}")
        return None
    
def add_full_alternative_record(row_id, new_data_dict):
    """
    Adds a new alternative row, populating it with all new stream outputs.

    Args:
        row_id (str): The base row ID for the alternative group.
        new_data_dict (dict): A dictionary containing the new raw data for all relevant columns.
    """
    try:
        # Fetch the original record (alt_id=0) to use as a template
        base_record_query = "SELECT * FROM `test_results` WHERE `row_id` = :row_id AND `alt_id` = 0 LIMIT 1"
        base_df = fetch_dataframe("llm", base_record_query, params={'row_id': row_id})

        if base_df is None or base_df.empty:
            logger.error(f"Could not find original record for row_id: {row_id}")
            return None

        updated_record = base_df.iloc[0].to_dict()

        # Find the highest current alt_id to determine the new one
        max_alt_id_query = "SELECT MAX(alt_id) as max_id FROM `test_results` WHERE `row_id` = :row_id"
        max_alt_df = fetch_dataframe("llm", max_alt_id_query, params={'row_id': row_id})
        
        new_alt_id = 0
        if max_alt_df is not None and not max_alt_df.empty:
            max_id = max_alt_df['max_id'].iloc[0]
            if pd.notna(max_id):
                new_alt_id = int(max_id) + 1

        # Update the record with new info and all new data fields
        updated_record.update(new_data_dict)
        updated_record['alt_id'] = new_alt_id
        
        # Ensure all values are JSON serializable where necessary
        for key, value in updated_record.items():
            if isinstance(value, (dict, list)):
                updated_record[key] = json.dumps(value)

        # Build and execute the INSERT query
        columns = ", ".join([f"`{col}`" for col in updated_record.keys() if col in base_df.columns])
        placeholders = ", ".join([f":{col}" for col in updated_record.keys() if col in base_df.columns])
        insert_query = f"INSERT INTO `test_results` ({columns}) VALUES ({placeholders})"
        
        valid_params = {k: v for k, v in updated_record.items() if k in base_df.columns}

        execute_query("llm", insert_query, params=valid_params)
        logger.info(f"Successfully added full alternative record for group '{row_id}'")
        return f"{row_id}-{new_alt_id}"

    except Exception as e:
        logger.error(f"Failed to add full alternative record for row_id {row_id}. Error: {e}")
        return None
