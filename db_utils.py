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

def get_db_connection():
    """
    Establishes and caches an SSH tunnel and database engine in Streamlit's session state.
    
    Returns:
        sqlalchemy.engine.Engine: The database engine object for executing queries.
        Returns None if the connection fails.
    """
    if 'db_tunnel' in st.session_state and st.session_state.db_tunnel.is_active:
        return st.session_state.db_engine

    try:
        logger.info("No active connection found. Establishing new SSH tunnel and DB engine...")

        db_user = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        ssh_user = st.secrets["SSH_USER"]
        ssh_password = st.secrets["SSH_PASSWORD"]
        ssh_host = st.secrets["SSH_HOST"]
        ssh_port = int(st.secrets["SSH_PORT"])
        db_name = "llm"
        remote_db_host = st.secrets["DB_HOST"]
        remote_db_port = int(st.secrets["DB_PORT"])

        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            remote_bind_address=(remote_db_host, remote_db_port),
        )
        tunnel.start()
        
        mysql_url = f"mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{db_name}"
        engine = create_engine(mysql_url)
        
        st.session_state.db_tunnel = tunnel
        st.session_state.db_engine = engine

        logger.info("Successfully established and cached SSH tunnel and DB connection.")
        
        return engine

    except Exception as e:
        logger.error(f"Failed to establish database connection. Error: {e}")
        # Clean up in case of failure
        if 'db_tunnel' in st.session_state:
            st.session_state.db_tunnel.stop()
            del st.session_state.db_tunnel
        if 'db_engine' in st.session_state:
            del st.session_state.db_engine
        return None

def fetch_dataframe(database_name, query, params=None):
    """
    Fetches data using the shared, cached database connection.
    
    Note: The 'database_name' argument is kept for compatibility with the original
    function signature but is no longer used as the connection is database-specific.
    """
    engine = get_db_connection()
    if engine is None:
        st.error("Database connection is not available.")
        return None

    try:
        with engine.connect() as connection:
            logger.info(f"Fetching data with query: {query[:100]}...")
            df = pd.read_sql(text(query), connection, params=params)
            logger.info(f"Successfully fetched {len(df)} rows.")
            return df
    except Exception as e:
        logger.error(f"Could not fetch data. Error: {e}")
        if 'db_tunnel' in st.session_state:
             st.session_state.db_tunnel.stop()
             del st.session_state.db_tunnel
        if 'db_engine' in st.session_state:
            del st.session_state.db_engine
        return None

def execute_query(database_name, query, params=None):
    """
    Executes a command using the shared, cached database connection.
    
    Note: The 'database_name' argument is kept for compatibility but is not used.
    """
    engine = get_db_connection()
    if engine is None:
        st.error("Database connection is not available.")
        return -1

    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:
                logger.info(f"Executing query: {query[:100]}...")
                result = connection.execute(text(query), params or {})
                transaction.commit()
                logger.info(f"Query executed successfully. {result.rowcount} rows affected.")
                return result.rowcount
    except Exception as e:
        logger.error(f"Failed to execute query. Error: {e}")
        if 'db_tunnel' in st.session_state:
             st.session_state.db_tunnel.stop()
             del st.session_state.db_tunnel
        if 'db_engine' in st.session_state:
            del st.session_state.db_engine
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
        base_record_query = "SELECT * FROM `test_results` WHERE `row_id` = :row_id AND `alt_id` = 0 LIMIT 1"
        base_df = fetch_dataframe("llm", base_record_query, params={'row_id': row_id})

        if base_df is None or base_df.empty:
            logger.error(f"Could not find original record for row_id: {row_id}")
            return None

        new_record = base_df.iloc[0].to_dict()

        max_alt_id_query = "SELECT MAX(alt_id) as max_id FROM `test_results` WHERE `row_id` = :row_id"
        max_alt_df = fetch_dataframe("llm", max_alt_id_query, params={'row_id': row_id})
        
        new_alt_id = 0
        if max_alt_df is not None and not max_alt_df.empty:
            max_id = max_alt_df['max_id'].iloc[0]
            if pd.notna(max_id):
                new_alt_id = int(max_id) + 1

        new_record['alt_id'] = new_alt_id
        new_record['id'] = f"{row_id}-{new_alt_id}"
        updated_record = new_record.copy()
        updated_record['alt_id'] = new_alt_id
        updated_record['id'] = f"{row_id}-{new_alt_id}"
        updated_record[column_to_update] = new_data
        
        for key, value in updated_record.items():
            if isinstance(value, (dict, list)):
                updated_record[key] = json.dumps(value)

        columns = ", ".join([f"`{col}`" for col in updated_record.keys() if col in base_df.columns and col != 'id'])
        placeholders = ", ".join([f":{col}" for col in updated_record.keys() if col in base_df.columns and col != 'id'])
        
        insert_query = f"INSERT INTO `test_results` ({columns}) VALUES ({placeholders})"
        
        valid_params = {k: v for k, v in updated_record.items() if k in base_df.columns and k != 'id'}

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
        base_record_query = "SELECT * FROM `test_results` WHERE `row_id` = :row_id AND `alt_id` = 0 LIMIT 1"
        base_df = fetch_dataframe("llm", base_record_query, params={'row_id': row_id})

        if base_df is None or base_df.empty:
            logger.error(f"Could not find original record for row_id: {row_id}")
            return None

        updated_record = base_df.iloc[0].to_dict()

        max_alt_id_query = "SELECT MAX(alt_id) as max_id FROM `test_results` WHERE `row_id` = :row_id"
        max_alt_df = fetch_dataframe("llm", max_alt_id_query, params={'row_id': row_id})
        
        new_alt_id = 0
        if max_alt_df is not None and not max_alt_df.empty:
            max_id = max_alt_df['max_id'].iloc[0]
            if pd.notna(max_id):
                new_alt_id = int(max_id) + 1

        updated_record.update(new_data_dict)
        updated_record['alt_id'] = new_alt_id
        
        for key, value in updated_record.items():
            if isinstance(value, (dict, list)):
                updated_record[key] = json.dumps(value)

        columns = ", ".join([f"`{col}`" for col in updated_record.keys() if col in base_df.columns and col != 'id'])
        placeholders = ", ".join([f":{col}" for col in updated_record.keys() if col in base_df.columns and col != 'id'])
        insert_query = f"INSERT INTO `test_results` ({columns}) VALUES ({placeholders})"
        
        valid_params = {k: v for k, v in updated_record.items() if k in base_df.columns and k != 'id'}

        execute_query("llm", insert_query, params=valid_params)
        logger.info(f"Successfully added full alternative record for group '{row_id}'")
        return f"{row_id}-{new_alt_id}"

    except Exception as e:
        logger.error(f"Failed to add full alternative record for row_id {row_id}. Error: {e}")
        return None