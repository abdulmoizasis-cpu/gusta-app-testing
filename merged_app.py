from helpers import *
from streams import *
from process_functions import *
from process_row import *
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(layout="wide")
st.title("Agentic-flow tester")
st.markdown("Click Run Analysis to start the tester.")

def main():
    try:
        with open("index.html", "r") as f:
            html_string = f.read()
            components.html(html_string, height=0, width=0) # height=0 makes it invisible
    except FileNotFoundError:
        st.warning("index.html not found. Button styling will not be applied.")
        
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'analysis_running' not in st.session_state:
        st.session_state.analysis_running = False
    if 'analysis_summary' not in st.session_state:
        st.session_state.analysis_summary = None

    df = db_utils.fetch_dataframe("llm", "SELECT * FROM test_results")

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
            future_to_row = {executor.submit(process_row, row['id'], row): row['id'] for index, row in df.iterrows()}
            
            processed_count = 0
            for future in as_completed(future_to_row):
                processed_count += 1
                result = future.result()
                live_results.append(result)

                if result:
                    if result.get('status') == 'deleted_duplicate':
                        deleted_count += 1
                    if result.get('failed'):
                        failed_count += 1
                
                with results_container:
                    display_result_expander(result, buttons_enabled=False)

                summary_placeholder.info(f"Processed: {processed_count}/{total_rows} | Failures: {failed_count} | Deleted: {deleted_count}")
                progress_bar.progress(processed_count / total_rows, text=f"Processing row {processed_count}/{total_rows}")

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

        for result in st.session_state.analysis_results:
            display_result_expander(result, buttons_enabled=True)

    elif df is None:
        st.error("Failed to load data from the database. Please check the connection and table name.")

if __name__ == "__main__":
    main()
