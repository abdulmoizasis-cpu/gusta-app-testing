from helpers import *
from streams import *
from process_functions import *
from process_row import *
from concurrent.futures import ThreadPoolExecutor, as_completed
import streamlit_nested_layout
import pandas as pd

st.set_page_config(layout="wide")
st.title("Agentic-flow tester")
st.markdown("Click Run Analysis to start the tester.")

def main():
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'analysis_running' not in st.session_state:
        st.session_state.analysis_running = False
    if 'analysis_summary' not in st.session_state:
        st.session_state.analysis_summary = None
    if 'df_to_process' not in st.session_state:
        st.session_state.df_to_process = None

    df = None
    max_retries = 3
    status_placeholder = st.empty()

    for attempt in range(max_retries):
        status_placeholder.info(f"⚙️ Connecting to the database... (Attempt {attempt + 1}/{max_retries})")
        df = db_utils.fetch_dataframe("llm", "SELECT * FROM test_results")
        if df is not None:
            status_placeholder.success("✅ Database connected successfully!")
            time.sleep(1.5) 
            status_placeholder.empty() 
            break 
        else:
            if attempt < max_retries - 1:
                time.sleep(2) 
            else:
                status_placeholder.error("❌ Database connection failed. Please refresh the page to try again.")
                st.stop() 
    st.success(f"Successfully loaded {len(df)} rows from the database.")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("Run Analysis", use_container_width=True):
            st.session_state.df_to_process = df
            st.session_state.analysis_running = True
            st.session_state.analysis_results = []
            st.session_state.analysis_summary = {}
            st.rerun()
    with col2:
        if st.button("Prepare ground truth", use_container_width=True):
            # Filter for rows where all three key columns are null/NaN
            empty_rows_df = df[df['ner_output'] == '']

            if empty_rows_df.empty:
                st.warning("✅ No empty rows to fill. Your ground truth is ready!")
            else:
                st.info(f"Found {len(empty_rows_df)} empty rows to process.")
                st.session_state.df_to_process = empty_rows_df
                st.session_state.analysis_running = True
                st.session_state.analysis_results = []
                st.session_state.analysis_summary = {}
                st.rerun()
        
    if st.session_state.analysis_running:
        st.header("Analysis in Progress...")
        analysis_start_time = time.time()
        df_to_process = st.session_state.df_to_process
        progress_bar = st.progress(0, text="Starting analysis...")
        summary_placeholder = st.empty()
        results_container = st.container()
        
        failed_count = 0
        deleted_count = 0
        total_rows = len(df_to_process)
        live_results = []
        latencies = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Group by row_id and submit each group for processing
            grouped = df_to_process.groupby('row_id')
            future_to_group = {executor.submit(process_row_group, row_id, group_df): row_id for row_id, group_df in grouped}
            
            processed_groups = 0
            total_groups = len(grouped)
            for future in as_completed(future_to_group):
                row_id = future_to_group[future]
                processed_groups += 1
                group_results, latency = future.result() # This will be a list of failed results for the group

                if latency > 0:
                    latencies.append((row_id,latency))

                if group_results:
                    # Extend the main results list with the list of failures from the group
                    live_results.extend(group_results)
                    failed_count += len(group_results)

                with results_container:
                    if group_results:
                        # Case 1: The group failed, but only has one alternative. Display it directly.
                        if len(group_results) == 1:
                            display_result_expander(group_results[0], buttons_enabled=False)
                        # Case 2: The group failed and has multiple alternatives. Create a nested view.
                        else:
                            row_id = group_results[0]['id'].split('-')[0]
                            with st.expander(f"🚨 Row ID: {row_id}"):
                                for result in group_results:
                                    # Create a nested expander for each specific ID
                                    with st.expander(f"ID: {result['id']}"):
                                        # Render the content directly inside the nested expander
                                        render_expander_content(result, buttons_enabled=False)

                processed_rows_count = sum(len(g) for _, g in list(grouped)[:processed_groups])
                summary_placeholder.info(f"Processed: {processed_rows_count}/{total_rows} rows ({processed_groups}/{total_groups} groups) | Failures: {failed_count}")
                progress_bar.progress(processed_groups / total_groups, text=f"Processing group {processed_groups}/{total_groups}")

        total_runtime = time.time() - analysis_start_time
        avg_latency = sum(lat for _, lat in latencies) / len(latencies) if latencies else 0
        if latencies:
            max_latency_row_id, max_latency = max(latencies, key=lambda item: item[1])
        else:
            max_latency_row_id, max_latency = "N/A", 0

        st.session_state.analysis_results = live_results
        st.session_state.analysis_summary = {
            "failed_count": failed_count,
            "total_runtime": total_runtime,
            "avg_latency": avg_latency,
            "max_latency": max_latency,
            "max_latency_row_id": max_latency_row_id
        }
        st.session_state.analysis_running = False
        st.rerun()

    elif st.session_state.analysis_results is not None:
        st.header("Analysis Results")
        summary = st.session_state.analysis_summary
        if summary:
            st.subheader("Performance Metrics")
            stat_cols = st.columns(4)
            with stat_cols[0]:
                st.metric(label="Total Run Time", value=f"{summary.get('total_runtime', 0):.2f} s")
            with stat_cols[1]:
                st.metric(label="Avg. Latency / Request", value=f"{summary.get('avg_latency', 0):.2f} s")
            with stat_cols[2]:
                st.metric(label="Max Latency", value=f"{summary.get('max_latency', 0):.2f} s")
            with stat_cols[3]:
                st.metric(label="Slowest Row ID", value=summary.get('max_latency_row_id', 'N/A'))

            st.markdown("---") # Add a separator

            if summary['failed_count'] > 0:
                st.warning(f"Found {summary['failed_count']} rows with significant differences.")
            else:
                st.success("✅ All rows passed the similarity checks!")

        results_df = pd.DataFrame(st.session_state.analysis_results)
        
        # Guard against empty results
        if not results_df.empty:
            # Create a row_id column for grouping
            results_df['row_id'] = results_df['id'].str.split('-').str[0]
            
            # Group by the new row_id
            grouped_results = results_df.groupby('row_id')

            for row_id, group_df in grouped_results:
                group_results = group_df.to_dict('records')
                
                # Case 1: The group failed, but only has one alternative. Display it directly.
                if len(group_results) == 1:
                    display_result_expander(group_results[0], buttons_enabled=True)
                # Case 2: The group failed and has multiple alternatives. Create a nested view.
                else:
                    with st.expander(f"🚨 Row Group: {row_id}"):
                        for result in group_results:
                            # Create a nested expander for each specific ID
                            with st.expander(f"ID: {result['id']}"):
                                # Render the content directly inside the nested expander
                                render_expander_content(result, buttons_enabled=True)
        else:
            st.success("✅ All rows have been cleared. Re-run analysis for fresh results.")

    elif df is None:
        st.error("Failed to load data from the database. Please check the connection and table name.")

if __name__ == "__main__":
    main()
