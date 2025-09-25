from process_functions import *
import pandas as pd

def process_row_group(row_id, group_df, use_agent_stream=False, stop_event=None):

    if stop_event and stop_event.is_set():
        return [], 0

    latency = 0
    new_ner_intent, new_ner_search_fields, new_chain_field_values, new_ner_date_filter= "", "", "", ""
    new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp = "", "", "", "", "", "", ""
    """
    Processes a group of rows (alternatives) for a single user_query.
    Makes one API call and compares the result against each alternative.
    """
    # Use the first row in the group to get the user_query and determine query type
    base_row = group_df.iloc[0]
    user_query = base_row.get('user_query', "")
    if not user_query:
        return [], 0
    
    if use_agent_stream:
        old_ner_raw = base_row.get('ner_output', "")
        old_ner = parse_csv_text_to_json(old_ner_raw) 
        old_ner_intent = ""
        if old_ner and isinstance(old_ner, dict):
            old_ner_intent = old_ner.get("intent", "")

        if old_ner_intent != ["search_list"]:
            return [], 0 
    
    existing_query_df = db_utils.fetch_dataframe(
        "llm",
        "SELECT row_id FROM `test_results` WHERE `user_query` = :user_query AND `row_id` != :current_row_id LIMIT 1",
        params={'user_query': user_query, 'current_row_id': row_id}
    )

    if existing_query_df is not None and not existing_query_df.empty:
        delete_query = "DELETE FROM `test_results` WHERE `row_id` = :row_id"
        db_utils.execute_query("llm", delete_query, params={'row_id': row_id})
        return [{
            "id": f"{row_id}-0",
            "failed": False,
            "status": "deleted_duplicate",
            "error": f"Deleted group '{row_id}': Duplicate of a query found in group '{existing_query_df.iloc[0]['row_id']}'"
        }], 0
    
    empty_rows_in_group = group_df[pd.isnull(group_df['ner_output'])]

    if not empty_rows_in_group.empty:
        check_query = "SELECT 1 FROM `test_results` WHERE `user_query` = :user_query AND `ner_output` IS NOT NULL LIMIT 1"
        established_df = db_utils.fetch_dataframe("llm", check_query, params={'user_query': api_query})
        if established_df is not None and not established_df.empty:
            ids_to_delete = empty_rows_in_group['id'].tolist()
            id_placeholders = ", ".join([f":id_{i}" for i in range(len(ids_to_delete))])
            delete_query = f"DELETE FROM `test_results` WHERE `id` IN ({id_placeholders})"
            params = {f"id_{i}": r_id for i, r_id in enumerate(ids_to_delete)}
            db_utils.execute_query("llm", delete_query, params=params)
            return [{
                "id": f"{row_id}-0",
                "failed": False,
                "status": "deleted_duplicate",
                "error": f"Deleted {len(ids_to_delete)} empty duplicate row(s) from group '{row_id}'."
            }], 0
    
    api_query = user_query
    query_type = "single"
    if '\n' in user_query.strip():
        query_type = "conversational"
        if '1.' not in user_query.strip() : 
            lines = user_query.strip().split('\n')
            formatted_lines = [f"{i}. {line.strip()}" for i, line in enumerate(lines, 1) if line.strip()]
            api_query = "\n".join(formatted_lines)
        else :
            lines = user_query.strip().split('\n')
            formatted_lines = []
            for line in lines:
                clean_line = line.strip()
                if clean_line:
                    parts = clean_line.split('.', 1)
                    if len(parts) == 2:
                        formatted_lines.append(parts[0].strip() + '.' + parts[1].lstrip())
                    else:
                        formatted_lines.append(clean_line)
            api_query = "\n".join(formatted_lines)

    if query_type == "conversational":
        new_ner_raw, new_search_raw, new_final_raw, new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values, latency = process_convo_row(api_query, row_id, user_query, None, None, use_agent_stream, stop_event)
    else:
        new_ner_raw, new_search_raw, new_final_raw, new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values, latency = process_single_row(api_query, row_id, user_query, None, None, use_agent_stream, stop_event)

    if new_ner_raw and isinstance(new_ner_raw, str) and (new_ner_raw.startswith("Conversational") or new_ner_raw.startswith("Retried")):
        return [{
            "id": f"{row_id}-0",
            "user_query": user_query,
            "failed": True,
            "failures": {"ner": True, "search": False, "final": False},
            "data": {
                "old_ner": parse_csv_text_to_json(base_row.get('ner_output', "")), "new_ner": new_ner,
                "old_search": convert_yaml_text_to_json(base_row.get('search_list_chain_output', "")), "new_search": new_search,
                "old_final": extract_url(base_row.get('final_output', "")), "new_final": new_final,
                "new_ner_raw": new_ner_raw, "new_search_raw": new_search_raw, "new_final_raw": new_final_raw
                }
        }], latency
    
    if new_ner_raw and isinstance(new_ner_raw, str) and "Process stopped externally" in new_ner_raw:
        return [], 0 
    
    any_match_found = False
    comparison_results = []
    

    # --- 2. Iterate through each alternative and compare ---
    for _, alt_row in group_df.iterrows():
        current_id = alt_row['id']
        old_ner_raw = alt_row.get('ner_output', "")
        old_search_raw = alt_row.get('search_list_chain_output', "")
        old_final_raw = alt_row.get('final_output', "")

        # --- Parse Old Data for this alternative ---
        old_ner = parse_csv_text_to_json(old_ner_raw)
        old_ner_intent, old_ner_search_fields, old_ner_leaf_entities, old_ner_date_filter = "", "", "", ""
        if old_ner:
            old_ner_intent = old_ner.get("intent", "")
            old_ner_search_fields = old_ner.get("search_fields", "")
            old_ner_leaf_entities = old_ner.get("leaf_entities", "")
            if old_ner_search_fields:
                old_ner_date_filter = [field.get("date_filter", "").get("value", "") for field in old_ner_search_fields if isinstance(field, dict)]
                old_ner_search_fields = [field for field in old_ner_search_fields if not isinstance(field, dict)]

        old_search = convert_yaml_text_to_json(old_search_raw)
        if old_search and "feedback_message" in old_search:
            old_search.pop("feedback_message")

        old_chain_field_values = ""
        if old_search:
            old_chain_search_fields = old_search.get("search_fields", "")
            old_chain_field_values = [item.get("field_value", "") for item in old_chain_search_fields if item.get("field_type", "") != "date"]
        
        if isinstance(old_final_raw, dict) :
            old_final = old_final_raw['url']
        elif old_final_raw :
            old_final = extract_url(old_final_raw)
        else:
            old_final = ""

        # --- Perform Comparison Logic ---
        ner_flag, search_flag, final_flag, date_flag = False, False, False, False

        ref_old_ner_search_fields, ref_new_ner_search_fields = remove_plural_pairs(old_ner_search_fields, new_ner_search_fields) if (bool(old_ner_search_fields) and bool(new_ner_search_fields)) else (old_ner_search_fields, new_ner_search_fields)
        ref_old_ner_leaf_entities, ref_new_ner_leaf_entities = remove_plural_pairs(old_ner_leaf_entities, new_ner_leaf_entities) if (bool(old_ner_leaf_entities) and bool(new_ner_leaf_entities)) else (old_ner_leaf_entities, new_ner_leaf_entities)
        ref_old_chain_field_values, ref_new_chain_field_values = remove_plural_pairs(old_chain_field_values, new_chain_field_values) if (bool(old_chain_field_values) and bool(new_chain_field_values)) else (old_chain_field_values, new_chain_field_values)

        date_flag = (bool(old_ner_date_filter) != bool(new_ner_date_filter))

        if (new_ner_intent != old_ner_intent) or (bool(old_ner_date_filter) != bool(new_ner_date_filter)) or (calculate_similarity(ref_old_ner_search_fields, ref_new_ner_search_fields)) or calculate_similarity(ref_old_ner_leaf_entities, ref_new_ner_leaf_entities):
            ner_flag = True
        
        search_flag = bool(set(ref_old_chain_field_values) ^ set(ref_new_chain_field_values)) or (bool(old_chain_field_values) != bool(new_chain_field_values))
        
        old_final_norm = str(old_final or '').strip()
        new_final_norm = str(new_final or '').strip()

        final_flag = (old_final_norm != new_final_norm)

        if final_flag and not ner_flag and not search_flag:
            if old_ner_date_filter and new_ner_date_filter:
                final_flag = False

        is_failure = ner_flag or search_flag or final_flag

        if not final_flag:
            is_failure = False 
            ner_flag = False
            search_flag = False

        is_new_row = (pd.isnull(old_ner_raw) or old_ner_raw == "") and \
                     (pd.isnull(old_search_raw) or old_search_raw == "") and \
                     (pd.isnull(old_final_raw) or old_final_raw == "")

        if is_new_row:
            updates = {
                'ner_output': json.dumps(new_ner_raw) if isinstance(new_ner_raw, (dict, list)) else new_ner_raw,
                'search_list_chain_output': json.dumps(new_search_raw) if isinstance(new_search_raw, (dict, list)) else new_search_raw,
                'final_output': json.dumps(new_final_raw) if isinstance(new_final_raw, (dict, list)) else new_final_raw,
                'query_type': query_type
            }
            update_database_record(current_id, updates)
            ner_flag, search_flag, final_flag = False, False, False
            is_failure = False
    

        print(f"\n--- Row ID: {current_id} ---")
        print(f"Old Chain Field Values: {old_chain_field_values}")
        print(f"New Chain Field Values: {new_chain_field_values}")
        print(f"Search flag :{search_flag}")
        print(f"----------------------------------------")
        print(f"old_ner_intent: {old_ner_intent}\n")
        print(f"new_ner_intent: {new_ner_intent}\n")

        print(f"old_ner_date_filter: {old_ner_date_filter}\n")
        print(f"new_ner_date_filter: {new_ner_date_filter}\n")
        print(f"Date flag : {date_flag}\n")

        print(f"old_ner_search_fields: {old_ner_search_fields}\n")
        print(f"new_ner_search_fields: {new_ner_search_fields}\n")
        # print(f"new_ner_search_fields_type:{bool(new_ner_search_fields)}")
        print(f"ref_old_ner_search_fields: {ref_old_ner_search_fields}\n")
        print(f"ref_new_ner_search_fields: {ref_new_ner_search_fields}\n")
        print(f"old_ner_leaf_entities: {ref_old_ner_leaf_entities}\n")
        print(f"new_ner_leaf_entities: {ref_new_ner_leaf_entities}\n")
        print(f"Ner flag :{ner_flag}\n")
        print(f"old_final: {old_final}\n")
        print(f"new_final: {new_final}\n")
        print(f"Final Flag : {final_flag}\n")
        # print(f"Ner flag :{ner_flag}\n")
        # print(f"Search flag :{search_flag}")


        # print(f"similarity between refs : {(calculate_similarity(ref_old_ner_search_fields, ref_new_ner_search_fields))}")
        # print(f"similarity between norm : {(calculate_similarity(old_ner_search_fields, new_ner_search_fields))}")
        
        if not is_failure:
            any_match_found = True
            break

        comparison_results.append({
            "id": current_id,
            "ner_flag": ner_flag,
            "search_flag": search_flag,
            "final_flag": final_flag
        })
            
    update_database_record(
        group_df['id'].tolist(), 
        {'time_stamp': new_time_stamp}
    )
    # If no match was found after checking all alternatives, the group has failed.
    if not any_match_found:
        failed_results = []
        # Use the recorded comparison results to build the final output
        for result in comparison_results:
            # Get the original row data corresponding to this result
            alt_row = group_df[group_df['id'] == result['id']].iloc[0]
            
            failed_results.append({
                "id": result['id'],
                "user_query": user_query,
                "failed": result['ner_flag'] or result['search_flag'] or result['final_flag'],
                "failures": {
                    "ner": result['ner_flag'],
                    "search": result['search_flag'],
                    "final": result['final_flag']
                },
                "data": {
                    "old_ner": parse_csv_text_to_json(alt_row.get('ner_output', "")), "new_ner": new_ner,
                    "old_search": convert_yaml_text_to_json(alt_row.get('search_list_chain_output', "")), "new_search": new_search,
                    "old_final": extract_url(alt_row.get('final_output', "")), "new_final": new_final,
                    "new_ner_raw": new_ner_raw, "new_search_raw": new_search_raw, "new_final_raw": new_final_raw
                }
            })
        return failed_results, latency

    # Otherwise, a match was found, and the group passes.
    return [], latency
