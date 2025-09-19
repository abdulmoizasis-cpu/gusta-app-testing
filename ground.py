# csv_uploader_app.py

import streamlit as st
import pandas as pd
import db_utils  # We will use the functions from your existing file
import time

# --- Database Interaction Functions ---

def check_row_id_exists(row_id):
    """
    Checks if a given row_id already exists in the test_results table.
    
    Args:
        row_id (str): The row_id to check.
        
    Returns:
        bool: True if the row_id exists, False otherwise.
    """
    query = "SELECT 1 FROM test_results WHERE row_id = :row_id LIMIT 1"
    df = db_utils.fetch_dataframe("llm", query, params={'row_id': row_id})
    return df is not None and not df.empty

def check_query_exists(user_query):
    """
    Checks if a given user_query already exists in the test_results table.
    
    Args:
        user_query (str): The user_query to check.
        
    Returns:
        str: The row_id of the existing query if found, otherwise None.
    """
    query = "SELECT row_id FROM test_results WHERE user_query = :user_query LIMIT 1"
    df = db_utils.fetch_dataframe("llm", query, params={'user_query': user_query})
    if df is not None and not df.empty:
        return df.iloc[0]['row_id']
    return None

def insert_new_record(record_data):
    """
    Inserts a new record into the test_results table.
    
    Args:
        record_data (dict): A dictionary containing the data for the new row.
        
    Returns:
        int: The number of rows affected (1 on success, -1 on failure).
    """
    query = """
    INSERT INTO test_results (row_id, user_query, query_type) 
    VALUES (:row_id, :user_query, :query_type)
    """
    return db_utils.execute_query("llm", query, params=record_data)

# --- Main Application Logic ---

def process_dataframe_in_batches(df):
    """
    Processes the uploaded dataframe in batches, performs validations,
    and inserts new records into the database.
    
    Args:
        df (pd.DataFrame): The dataframe loaded from the user's CSV.
    """
    
    # Validate that the necessary columns exist in the uploaded file
    if not {'user_query', 'row_id'}.issubset(df.columns):
        st.error("❌ Your CSV file must contain both 'user_query' and 'row_id' columns.")
        return

    total_rows = len(df)
    batch_size = 5
    
    # Set up Streamlit elements for live updates
    progress_bar = st.progress(0, text="Starting processing...")
    summary_placeholder = st.empty()
    results_placeholder = st.container()

    # Initialize counters for the final summary
    inserted_count = 0
    skipped_row_id_count = 0
    skipped_query_count = 0
    failed_count = 0
    skipped_rows = []

    for i in range(0, total_rows, batch_size):
        batch_df = df.iloc[i:i + batch_size]
        
        for index, row in batch_df.iterrows():
            # Calculate the actual row number for user feedback
            row_index_in_file = i + index - batch_df.index[0] + 1
            user_query = row['user_query']
            row_id = str(row['row_id']) # Ensure row_id is treated as a string

            # 1. Validation: Check if row_id already exists
            if check_row_id_exists(row_id):
                skipped_row_id_count += 1
                reason = f"Row ID '{row_id}' already exists in the database."
                skipped_rows.append({'Row in File': row_index_in_file, 'row_id': row_id, 'user_query': user_query, 'Reason': reason})
                with results_placeholder:
                    st.warning(f"⚠️ Row {row_index_in_file}: Skipped. {reason}")
                continue
            
            # 2. Validation: Check if the user_query is a duplicate
            existing_row_id = check_query_exists(user_query)
            if existing_row_id:
                skipped_query_count += 1
                reason = f"Query is a duplicate of one in existing row ID '{existing_row_id}'."
                skipped_rows.append({'Row in File': row_index_in_file, 'row_id': row_id, 'user_query': user_query, 'Reason': reason})
                with results_placeholder:
                    st.warning(f"⚠️ Row {row_index_in_file}: Skipped. {reason}")
                continue

            # 3. Logic: Determine the query_type
            query_type = "conversational" if '\n' in user_query.strip() else "single"
            
            # 4. Action: Prepare the data and insert the new record
            new_record = {
                'row_id': row_id,
                'user_query': user_query,
                'query_type': query_type,
            }
            
            result = insert_new_record(new_record)
            
            if result > 0:
                inserted_count += 1
                with results_placeholder:
                    st.success(f"✅ Row {row_index_in_file}: Successfully inserted record with row_id '{row_id}'.")
            else:
                failed_count +=1
                reason = "An error occurred during database insertion."
                skipped_rows.append({'Row in File': row_index_in_file, 'row_id': row_id, 'user_query': user_query, 'Reason': reason})
                with results_placeholder:
                    st.error(f"❌ Row {row_index_in_file}: {reason}")

        # Update the progress bar and summary text
        processed_rows = i + len(batch_df)
        progress_percentage = processed_rows / total_rows
        progress_bar.progress(progress_percentage, text=f"Processed {processed_rows} of {total_rows} rows...")
        summary_placeholder.info(f"📊 **Live Summary:** Inserted: {inserted_count} | Skipped (Duplicate ID): {skipped_row_id_count} | Skipped (Duplicate Query): {skipped_query_count} | Failed: {failed_count}")
        time.sleep(0.5) # A small delay to make the UI feel smoother

    # Final summary after processing is complete
    st.success("🎉 Processing complete!")
    st.subheader("Final Summary")
    st.markdown(f"- ✅ **Successfully Inserted:** {inserted_count} rows")
    st.markdown(f"- ⚠️ **Skipped (Duplicate Row ID):** {skipped_row_id_count} rows")
    st.markdown(f"- ⚠️ **Skipped (Duplicate Query):** {skipped_query_count} rows")
    st.markdown(f"- ❌ **Failed Inserts:** {failed_count} rows")
    
    if skipped_rows:
        st.subheader("Details of Skipped/Failed Rows")
        st.dataframe(pd.DataFrame(skipped_rows))

def main():
    """Main function to set up and run the Streamlit application."""
    st.set_page_config(layout="wide")
    st.title("🚀 CSV to Database Uploader")
    
    st.markdown("""
    Upload a CSV file with `user_query` and `row_id` columns to add new test cases to the database.
    - The app processes 5 rows at a time to stay responsive.
    - It checks for **duplicate `row_id`** and **duplicate `user_query`** before inserting.
    - It automatically detects the `query_type` ('single' or 'conversational').
    """)

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.success("File uploaded successfully. Here's a preview:")
            st.dataframe(df.head())

            if st.button("Start Processing", use_container_width=True, type="primary"):
                with st.spinner('Connecting to database and processing...'):
                    process_dataframe_in_batches(df)

        except Exception as e:
            st.error(f"An error occurred while reading the file: {e}")

if __name__ == "__main__":
    # Check for a valid database connection on startup and display status in the sidebar
    try:
        engine = db_utils.get_db_engine("llm")
        if engine:
            st.sidebar.success("✅ Database connection successful!")
        else:
            st.sidebar.error("❌ Database connection failed. Check logs.")
    except Exception as e:
        st.sidebar.error(f"DB connection error: {e}")

    main()