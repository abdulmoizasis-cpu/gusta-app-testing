import streamlit as st
import pandas as pd
import requests
import json
import yaml
import re
import difflib
import ast
from concurrent.futures import ThreadPoolExecutor, as_completed
import db_utils 
import datetime

DB_NAME = "llm"
TABLE_NAME = "test_results"

API_STREAM_URL = "https://aitest.ebalina.com/stream"

st.set_page_config(layout="wide")
st.title("🚀 API Response Comparison Tool")
st.markdown("This tool compares data from a database against live API calls to identify and highlight significant differences.")

def parse_csv_text_to_json(text_from_csv):
    if not isinstance(text_from_csv, str) or not text_from_csv.strip():
        return None
    try:
        return json.loads(text_from_csv)
    except json.JSONDecodeError:
        try:
            return ast.literal_eval(text_from_csv)
        except (ValueError, SyntaxError):
            return {"raw_unparseable_text": text_from_csv}

def convert_yaml_text_to_json(yaml_text):
    if not isinstance(yaml_text, str) or not yaml_text.strip():
        return {}
    try:
        parsed_data = yaml.safe_load(yaml_text)
        return parsed_data if isinstance(parsed_data, dict) else {"parsed_content": parsed_data}
    except yaml.YAMLError:
        return {"raw_unparseable_text": yaml_text}

def get_api_results_from_stream(query_text):
    payload = {"query": query_text, "k": 5}
    try:
        response = requests.post(API_STREAM_URL, json=payload, stream=True, timeout=90)
        response.raise_for_status()

        full_response_data = []
        for line in response.iter_lines():
            if line:
                json_line = line.decode('utf-8').replace('data: ', '').strip()
                if json_line:
                    try:
                        full_response_data.append(json.loads(json_line))
                    except json.JSONDecodeError:
                        pass
        
        ner_output, final_output, search_list_chain_output = "", "", ""
        if not full_response_data:
            return None, None, None

        for item in full_response_data:
            if item.get("log_title") == "NER Succeded":
                content = item.get("content")
                ner_output = json.dumps(content) if isinstance(content, (dict, list)) else str(content)
            if item.get("log_title") == "Search List Result":
                content = item.get("content")
                search_list_chain_output = json.dumps(content) if isinstance(content, (dict, list)) else str(content)

        time_stamp = full_response_data[0].get("timestamp")
        time_stamp = datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
        
        final_output = full_response_data[-1].get("output", "")
        return ner_output, final_output, search_list_chain_output, time_stamp

    except requests.exceptions.RequestException:
        return None, None, None

def jaccard_similarity(dict1, dict2):
    def _process_dict_to_set(data_dict):
        processed_set = set()
        def _flatten(obj, prefix=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _flatten(v, f"{prefix}_{k}" if prefix else k)
            elif isinstance(obj, list):
                for item in obj:
                    _flatten(item, prefix)
            else:
                processed_set.add((prefix, obj))
        _flatten(data_dict)
        return processed_set

    set1 = _process_dict_to_set(dict1 or {})
    set2 = _process_dict_to_set(dict2 or {})
    intersection = set1.intersection(set2)
    union = set1.union(set2)
    return len(intersection) / len(union) if union else 1.0

def extract_url(text_data):
    if isinstance(text_data, dict):
        return text_data.get('url') 

    if not isinstance(text_data, str):
        return text_data

    match = re.search(r"['\"]?url['\"]?\s*:\s*['\"]?([^'\"\s]+)['\"]?", text_data, re.IGNORECASE)
    return match.group(1) if match else text_data

def get_diff(text1, text2):
    lines1 = text1.splitlines()
    lines2 = text2.splitlines()
    matcher = difflib.SequenceMatcher(None, lines1, lines2)
    return matcher.get_opcodes()

def render_diff(opcodes, lines1, lines2):
    left_html, right_html = [], []
    style = "white-space: pre-wrap; font-family: monospace; padding: 5px; border-radius: 5px; margin-bottom: 2px;"
    delete_style = f"background-color: #ffdddd; color: #111; {style}"
    insert_style = f"background-color: #ddffdd; color: #111; {style}"

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == 'equal':
            for line in lines1[i1:i2]:
                left_html.append(f'<div style="{style}">{line or "&nbsp;"}</div>')
            for line in lines2[j1:j2]:
                right_html.append(f'<div style="{style}">{line or "&nbsp;"}</div>')
        elif tag == 'delete':
            for line in lines1[i1:i2]:
                left_html.append(f'<div style="{delete_style}">{line or "&nbsp;"}</div>')
                right_html.append(f'<div style="{style}">&nbsp;</div>')
        elif tag == 'insert':
            for line in lines2[j1:j2]:
                left_html.append(f'<div style="{style}">&nbsp;</div>')
                right_html.append(f'<div style="{insert_style}">{line or "&nbsp;"}</div>')
        elif tag == 'replace':
            len1, len2 = i2 - i1, j2 - j1
            max_len = max(len1, len2)
            for i in range(max_len):
                if i < len1:
                    left_html.append(f'<div style="{delete_style}">{lines1[i1+i] or "&nbsp;"}</div>')
                else:
                    left_html.append(f'<div style="{style}">&nbsp;</div>')
                if i < len2:
                    right_html.append(f'<div style="{insert_style}">{lines2[j1+i] or "&nbsp;"}</div>')
                else:
                    right_html.append(f'<div style="{style}">&nbsp;</div>')

    return "".join(left_html), "".join(right_html)

def update_database_record(record_id, updates):
    if not updates:
        return

    set_clauses = ", ".join([f"`{col}` = :{col}" for col in updates.keys()])
    query = f"UPDATE `{TABLE_NAME}` SET {set_clauses} WHERE `id` = :id"
    
    params = updates.copy()
    params['id'] = record_id
    
    db_utils.execute_query(DB_NAME, query, params)

def process_row(index, row):
    user_query = row.get('user_query', "")
    if not user_query:
        return None

    old_ner_raw = row.get('ner_output', "")
    old_search_raw = row.get('search_list_chain_output', "")
    old_final_raw = row.get('final_output', "")

    is_new_row = (pd.isnull(old_ner_raw) or old_ner_raw == "") and \
                 (pd.isnull(old_search_raw) or old_search_raw == "") and \
                 (pd.isnull(old_final_raw) or old_final_raw == "")

    api_query = user_query
    query_type = None

    if is_new_row:
        if '\n' in user_query.strip():
            query_type = "conversational"
            lines = user_query.strip().split('\n')
            formatted_lines = [f"{i}. {line.strip()}" for i, line in enumerate(lines, 1) if line.strip()]
            api_query = "\n".join(formatted_lines)
        else:
            query_type = "single"

        existing_query_df = db_utils.fetch_dataframe(
            DB_NAME,
            f"SELECT id FROM `{TABLE_NAME}` WHERE `user_query` = :user_query AND `id` != :current_id AND (`ner_output` IS NOT NULL AND `ner_output` != '')",
            params={'user_query': api_query, 'current_id': index}
        )

        if existing_query_df is not None and not existing_query_df.empty:
            delete_query = f"DELETE FROM `{TABLE_NAME}` WHERE `id` = :id"
            db_utils.execute_query(DB_NAME, delete_query, params={'id': index})
            return {
                "id": index,
                "failed": False,  # Not a failure, just skipped
                "status": "skipped_duplicate",
                "error": f"Skipped: Duplicate of a processed query found in row ID {existing_query_df.iloc[0]['id']}"
            }


    old_ner = parse_csv_text_to_json(old_ner_raw)
    if old_ner :
        old_ner.pop("possible_answer", None)
    
    old_search = convert_yaml_text_to_json(old_search_raw)
    if old_search and "feedback_message" in old_search:
        old_search.pop("feedback_message")

    if isinstance(old_final_raw, dict) :
        old_final = old_final_raw['url']
    elif old_final_raw :
        old_final = extract_url(old_final_raw)
    else:
        old_final = ""

    new_ner_raw, new_final_raw, new_search_raw, new_time_stamp = get_api_results_from_stream(api_query)

    if new_ner_raw is None:
        return {"id": index, "failed": True, "error": "API Request Failed"}

    new_ner = parse_csv_text_to_json(new_ner_raw)
    if new_ner:
        new_ner.pop("possible_answer", None)

    new_search = convert_yaml_text_to_json(new_search_raw)
    if new_search and "feedback_message" in new_search:
        new_search.pop("feedback_message")
        
    if isinstance(new_final_raw, dict) :
       new_final = new_final_raw['url']
    else :
        new_final = extract_url(new_final_raw)
    
    ner_similarity = jaccard_similarity(old_ner, new_ner)
    search_similarity = jaccard_similarity(old_search, new_search)

    ner_flag = ner_similarity >= 0.9
    search_flag = search_similarity >= 0.9
    final_flag = old_final == new_final

    updates_to_make = {}
    if pd.isnull(old_ner_raw) or old_ner_raw == "":
        updates_to_make['ner_output'] = new_ner_raw
    if pd.isnull(old_search_raw) or old_search_raw == "":
        updates_to_make['search_list_chain_output'] = new_search_raw
    if pd.isnull(old_final_raw) or old_final_raw == "":
        updates_to_make['final_output'] = new_final_raw
    if query_type:
        updates_to_make['query_type'] = query_type
        if query_type == "conversational":
            updates_to_make['user_query'] = api_query
    
    if updates_to_make:
        updates_to_make['time_stamp'] = new_time_stamp
        update_database_record(index, updates_to_make)

    return {
        "id": index,
        "failed": not (ner_flag and search_flag and final_flag),
        "updates": updates_to_make,
        "failures": {
            "ner": not ner_flag,
            "search": not search_flag,
            "final": not final_flag
        },
        "data": {
            "old_ner": old_ner, "new_ner": new_ner,
            "old_search": old_search, "new_search": new_search,
            "old_final": old_final, "new_final": new_final
        }
    }

def display_diff(title, old_data, new_data):
    st.subheader(title)
    
    if isinstance(old_data, (dict, list)):
        old_text = json.dumps(old_data, indent=4, sort_keys=True)
    else:
        old_text = str(old_data or "")

    if isinstance(new_data, (dict, list)):
        new_text = json.dumps(new_data, indent=4, sort_keys=True)
    else:
        new_text = str(new_data or "")

    lines1 = old_text.splitlines()
    lines2 = new_text.splitlines()
    opcodes = get_diff(old_text, new_text)
    
    left_html, right_html = render_diff(opcodes, lines1, lines2)
    left_col, right_col = st.columns(2)
    with left_col:
        st.markdown("<h5>Original</h5>", unsafe_allow_html=True)
        st.markdown(left_html, unsafe_allow_html=True)
    with right_col:
        st.markdown("<h5>New</h5>", unsafe_allow_html=True)
        st.markdown(right_html, unsafe_allow_html=True)

def main():
    df = db_utils.fetch_dataframe(DB_NAME, f"SELECT * FROM {TABLE_NAME}")

    if df is not None:
        st.success(f"Successfully loaded {len(df)} rows from the database.")

        if st.button("Run Analysis", use_container_width=True):
            progress_bar = st.progress(0, text="Starting analysis...")
            header_placeholder = st.empty()
            summary_placeholder = st.empty()
            results_container = st.container()

            header_placeholder.header("Analysis in Progress...")
            
            failed_count = 0
            deleted_count = 0
            total_rows = len(df)
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_row = {executor.submit(process_row, row['id'], row): row['id'] for index, row in df.iterrows()}
                
                for i, future in enumerate(as_completed(future_to_row), 1):
                    result = future.result()
                    if not result:
                        continue

                    if result.get('status') == 'deleted_duplicate':
                        deleted_count +=1
                        st.error(f"Row ID {result['id']}: {result['error']}")
                        summary_placeholder.info(f"Processed: {i}/{total_rows} | Failures: {failed_count} | Deleted: {deleted_count}")
                        progress_bar.progress(i / total_rows, text=f"Processing row {i}/{total_rows}")
                        continue

                    if result.get('failed'):
                        failed_count += 1
                        with results_container:
                            with st.expander(f"🚨 Row ID: {result['id']}"):
                                if result.get('error'):
                                    st.error(f"Could not process row: {result['error']}")
                                    continue
                                
                                if result["failures"]["ner"]:
                                    display_diff("NER Output Difference", result["data"]["old_ner"], result["data"]["new_ner"])
                                    st.divider()
                                
                                if result["failures"]["search"]:
                                    display_diff("Search Output Difference", result["data"]["old_search"], result["data"]["new_search"])
                                    st.divider()

                                if result["failures"]["final"]:
                                    display_diff("Final Output Difference", result["data"]["old_final"], result["data"]["new_final"])
                    
                    summary_placeholder.info(f"Processed: {i}/{total_rows} | Failures: {failed_count} | Deleted: {deleted_count}")
                    progress_bar.progress(i / total_rows, text=f"Processing row {i}/{total_rows}")
            
            header_placeholder.header("Analysis Results")
            if failed_count > 0:
                summary_placeholder.warning(f"Found {failed_count} rows with significant differences.")
            else:
                summary_placeholder.success("✅ All rows passed the similarity checks!")
    else:
        st.error("Failed to load data from the database. Please check the connection and table name.")

if __name__ == "__main__":
    main()