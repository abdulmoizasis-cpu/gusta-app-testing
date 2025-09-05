import os
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import logging
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from sqlalchemy import text

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
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        ssh_user = os.getenv("SSH_USER")
        ssh_password = os.getenv("SSH_PASSWORD")

        if not all([db_user, db_password, ssh_user, ssh_password]):
            raise ValueError("Required environment variables (DB_USER, DB_PASSWORD, SSH_USER, SSH_PASSWORD) are not set.")

        # Define SSH tunnel and remote database parameters
        ssh_host = "ebalina.com"
        ssh_port = 5322
        remote_db_host = "69.167.186.10"
        remote_db_port = 3306

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
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        ssh_user = os.getenv("SSH_USER")
        ssh_password = os.getenv("SSH_PASSWORD")

        if not all([db_user, db_password, ssh_user, ssh_password]):
            raise ValueError("Required environment variables (DB_USER, DB_PASSWORD, SSH_USER, SSH_PASSWORD) are not set.")

        # Define SSH tunnel and remote database parameters
        ssh_host = "ebalina.com"
        ssh_port = 5322
        remote_db_host = "69.167.186.10"
        remote_db_port = 3306
        
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