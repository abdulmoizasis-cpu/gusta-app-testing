import os
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import logging
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
import json
from sqlalchemy import text
import streamlit as st

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
logging.getLogger('sqlalchemy.pool').setLevel(logging.INFO)

@st.cache_resource
def init_connection_manager():
    """
    Initializes a dictionary that will be cached across reruns 
    to hold our active database connections (engine and tunnel).
    """
    return {}

def get_db_engine(database_name):
    """
    Creates or retrieves a database engine and its SSH tunnel.
    
    Checks if the connection is active and re-establishes it if not.
    """
    _connections = init_connection_manager()

    if database_name in _connections and _connections[database_name]['tunnel'].is_active:
        return _connections[database_name]['engine']

    logger.info(f"Establishing new SSH tunnel and DB engine for '{database_name}'...")
    try:
        # ... (your existing credential and tunnel creation logic) ...
        db_user = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        ssh_user = st.secrets["SSH_USER"]
        ssh_password = st.secrets["SSH_PASSWORD"]
        ssh_host = st.secrets["SSH_HOST"]
        ssh_port = st.secrets["SSH_PORT"]
        remote_db_host = st.secrets["DB_HOST"]
        remote_db_port = st.secrets["DB_PORT"]

        tunnel = SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_password=ssh_password,
            remote_bind_address=(remote_db_host, remote_db_port),
        )
        tunnel.start()
        logger.info("SSH tunnel established successfully.")

        mysql_url = f"mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{tunnel.local_bind_port}/{database_name}"
        engine = create_engine(mysql_url, pool_recycle=10, pool_pre_ping=True)

        _connections[database_name] = {'engine': engine, 'tunnel': tunnel}
        
        return engine

    except Exception as e:
        logger.error(f"Failed to create database engine for '{database_name}'. Error: {e}")
        if 'tunnel' in locals() and tunnel.is_active:
            tunnel.stop()
        return None
    
def fetch_dataframe(database_name, query, params=None):
    """
    Connects to a database using the managed engine and fetches data.
    """
    try:
        engine = get_db_engine(database_name)
        if engine is None:
            raise ConnectionError("Failed to get a database engine.")

        with engine.connect() as connection:
            logger.info(f"Fetching data from '{database_name}'...")
            df = pd.read_sql(text(query), connection, params=params)
            logger.info(f"Successfully fetched {len(df)} rows.")
            return df

    except Exception as e:
        logger.error(f"Could not fetch data from '{database_name}'. Error: {e}")
        return None


def execute_query(database_name, query, params=None):
    """
    Connects to a database using the managed engine and executes a command.
    """
    try:
        engine = get_db_engine(database_name)
        if engine is None:
            raise ConnectionError("Failed to get a database engine.")
            
        with engine.connect() as connection:
            with connection.begin() as transaction:
                try:
                    logger.info(f"Executing query on '{database_name}'...")
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