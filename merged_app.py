from helpers3 import *
from streams import *
from process_functions import *
from process_row2 import *
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

    df = db_utils2.fetch_dataframe("llm", "SELECT * FROM test_results")

    if df is not None:
        st.success(f"Successfully loaded {len(df)} rows from the database.")
        if st.button("Run Analysis", use_container_width=True):
            st.session_state.analysis_running = True
            st.session_state.analysis_results = []
            st.session_state.analysis_summary = {}
            st.rerun()

    if st.session_state.analysis_running:
        st.header("Analysis in Progress...")
        progress_bar = st.progress(0, text="Starting analysis...")
        summary_placeholder = st.empty()
        results_container = st.container()
        
        failed_count = 0
        deleted_count = 0
        total_rows = len(df)
        live_results = []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Group by row_id and submit each group for processing
            grouped = df.groupby('row_id')
            future_to_group = {executor.submit(process_row_group, row_id, group_df): row_id for row_id, group_df in grouped}
            
            processed_groups = 0
            total_groups = len(grouped)
            for future in as_completed(future_to_group):
                processed_groups += 1
                group_results = future.result() # This will be a list of failed results for the group

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

        st.session_state.analysis_results = live_results
        st.session_state.analysis_summary = {"failed_count": failed_count}
        st.session_state.analysis_running = False
        st.rerun()

    elif st.session_state.analysis_results is not None:
        st.header("Analysis Results")
        summary = st.session_state.analysis_summary
        if summary:
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

    elif df is None:
        st.error("Failed to load data from the database. Please check the connection and table name.")

if __name__ == "__main__":
    main()
