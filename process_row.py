from process_functions import *
import pandas as pd

def process_row(index, row):
    user_query = row.get('user_query', "")
    if not user_query:
        return None

    old_ner_raw = row.get('ner_output', "")
    old_search_raw = row.get('search_list_chain_output', "")
    old_final_raw = row.get('final_output', "")
    old_ner_intent, new_ner_intent, old_ner_search_fields, new_ner_search_fields, old_chain_field_values, new_chain_field_values, new_ner_date_filter, old_ner_date_filter = "", "", "", "", "", "", "", ""
    ref_new_chain_field_values,ref_new_ner_leaf_entities, ref_new_ner_search_fields, ref_old_chain_field_values, ref_old_ner_leaf_entities, ref_old_ner_search_fields = "", "", "", "", "", ""
    old_ner_leaf_entities, new_ner_leaf_entities = "", ""

    is_new_row = (pd.isnull(old_ner_raw) or old_ner_raw == "") and \
                 (pd.isnull(old_search_raw) or old_search_raw == "") and \
                 (pd.isnull(old_final_raw) or old_final_raw == "")

    api_query = user_query
    query_type = None

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
    else:
        query_type = "single"

    existing_query_df = db_utils.fetch_dataframe(
        "llm",
        "SELECT id FROM `test_results` WHERE `user_query` = :user_query AND `id` != :current_id",
        params={'user_query': api_query, 'current_id': index}
    )

    if existing_query_df is not None and not existing_query_df.empty:
        delete_query = "DELETE FROM `test_results` WHERE `id` = :id"
        db_utils.execute_query("llm", delete_query, params={'id': index})
        return {
            "id": index,
            "failed": False,
            "status": "skipped_duplicate",
            "error": f"Skipped: Duplicate of a processed query found in row ID {existing_query_df.iloc[0]['id']}"
        }

    old_ner = parse_csv_text_to_json(old_ner_raw)
    if old_ner:
        old_ner_intent = old_ner.get("intent", "")
        old_ner_search_fields = old_ner.get("search_fields", "")
        old_ner_leaf_entities = old_ner.get("leaf_entities", "")
        if old_ner_search_fields:
            old_ner_date_filter =[field.get("date_filter", "").get("value", "") for field in old_ner_search_fields if isinstance(field, dict)]
            old_ner_search_fields = [field for field in old_ner_search_fields if not isinstance(field, dict)]
    
    old_search = convert_yaml_text_to_json(old_search_raw)
    if old_search and "feedback_message" in old_search:
        old_search.pop("feedback_message")

    if old_search :
        old_chain_search_fields = old_search.get("search_fields", "")
        old_chain_field_values = [item.get("field_value", "") for item in old_chain_search_fields if item.get("field_type", "") != "date"]

    if isinstance(old_final_raw, dict) :
        old_final = old_final_raw['url']
    elif old_final_raw :
        old_final = extract_url(old_final_raw)
    else:
        old_final = ""

    if query_type == "conversational" :
        new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values = process_convo_row(api_query, index, user_query, old_ner, old_ner_intent)
    elif query_type == "single" :
        new_ner_raw, new_search_raw, new_final_raw, new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values = process_single_row(api_query, index, user_query, old_ner, old_ner_intent)
    
    if new_ner_raw and isinstance(new_ner_raw, str) and (new_ner_raw.startswith("Conversational") or new_ner_raw.startswith("Retried")): 
        return {
            "id": index,
            "user_query": user_query,
            "failed": True,
            "failures": {
                "ner": True,
                "search": False,
                "final": False
                },
            "data": {
                "old_ner": old_ner,
                "new_ner_raw": new_ner_raw}
            }
    
    ner_flag = False
    final_flag = False
    search_flag= False

    ref_old_ner_search_fields = old_ner_search_fields
    ref_new_ner_search_fields = new_ner_search_fields
    
    ref_old_ner_leaf_entities = old_ner_leaf_entities
    ref_new_ner_leaf_entities = new_ner_leaf_entities

    ref_old_chain_field_values = old_chain_field_values
    ref_new_chain_field_values = new_chain_field_values

    if bool(old_ner_search_fields) and bool(new_ner_search_fields) :
        ref_old_ner_search_fields, ref_new_ner_search_fields = remove_plural_pairs(old_ner_search_fields, new_ner_search_fields)

    if bool(old_ner_leaf_entities) and bool(new_ner_leaf_entities) :
        ref_old_ner_leaf_entities, ref_new_ner_leaf_entities = remove_plural_pairs(old_ner_leaf_entities, new_ner_leaf_entities)

    if bool(old_chain_field_values) and bool(new_chain_field_values) :
        ref_old_chain_field_values, ref_new_chain_field_values = remove_plural_pairs(old_chain_field_values, new_chain_field_values)

    if (new_ner_intent != old_ner_intent) or (bool(old_ner_date_filter) != bool(new_ner_date_filter)) or (calculate_similarity(ref_old_ner_search_fields, ref_new_ner_search_fields)) or calculate_similarity(ref_old_ner_leaf_entities, ref_new_ner_leaf_entities):
        ner_flag = True

    search_flag = bool(set(ref_old_chain_field_values) ^ set(ref_new_chain_field_values)) or (bool(old_chain_field_values) != bool(new_chain_field_values))

    # print(f"\n--- Row ID: {index} ---")
    # print(f"Old Chain Field Values: {old_chain_field_values}")
    # print(f"New Chain Field Values: {new_chain_field_values}")
    # print(f"Search flag :{search_flag}")
    # print(f"----------------------------------------")
    # print(f"old_ner_intent: {old_ner_intent}")
    # print(f"new_ner_intent: {new_ner_intent}\n")
    # print(f"old_ner_date_filter: {old_ner_date_filter}")
    # print(f"new_ner_date_filtert: {new_ner_date_filter}\n")
    # print(f"old_ner_search_fields: {old_ner_search_fields}")
    # print(f"new_ner_search_fields: {new_ner_search_fields}\n")
    # print(f"new_ner_search_fields_type:{bool(new_ner_search_fields)}")
    # print(f"ref_old_ner_search_fields: {ref_old_ner_search_fields}")
    # print(f"ref_new_ner_search_fields: {ref_new_ner_search_fields}\n")
    # print(f"old_ner_leaf_entities: {ref_old_ner_leaf_entities}")
    # print(f"new_ner_leaf_entities: {ref_new_ner_leaf_entities}\n")
    # print(f"Ner flag :{ner_flag}")

    # print(f"similarity between refs : {(calculate_similarity(ref_old_ner_search_fields, ref_new_ner_search_fields))}")
    # print(f"similarity between norm : {(calculate_similarity(old_ner_search_fields, new_ner_search_fields))}")


    if (ner_flag or search_flag) :
        if (old_final != new_final) :
            final_flag = True

    updates_to_make = {}
    updates_to_make['time_stamp'] = new_time_stamp

    if query_type == "conversational" :
        updates_to_make['time_stamp'] = new_time_stamp
        if pd.isnull(old_ner_raw) or old_ner_raw == "":
            updates_to_make['ner_output'] = json.dumps(new_ner_raw) if isinstance(new_ner_raw, (dict, list)) else new_ner_raw
        if pd.isnull(old_search_raw) or old_search_raw == "":
            updates_to_make['search_list_chain_output'] = json.dumps(new_search_raw) if isinstance(new_search_raw, (dict, list)) else new_search_raw
        if pd.isnull(old_final_raw) or old_final_raw == "":
            updates_to_make['final_output'] = json.dumps(new_final_raw) if isinstance(new_final_raw, (dict, list)) else new_final_raw
        if query_type:
            updates_to_make['query_type'] = query_type
            updates_to_make['user_query'] = api_query
    else :
        updates_to_make['time_stamp'] = new_time_stamp
        if pd.isnull(old_ner_raw) or old_ner_raw == "":
            updates_to_make['ner_output'] = new_ner_raw
        if pd.isnull(old_search_raw) or old_search_raw == "":
            updates_to_make['search_list_chain_output'] = new_search_raw
        if pd.isnull(old_final_raw) or old_final_raw == "":
            updates_to_make['final_output'] = new_final_raw
        if query_type:
            updates_to_make['query_type'] = query_type

    if updates_to_make:
        update_database_record(index, updates_to_make)

    return {
        "id": index,
        "user_query": user_query,
        "failed": (search_flag or ner_flag or final_flag),
        "updates": updates_to_make,
        "failures": {
            "ner": ner_flag,
            "search": search_flag,
            "final": final_flag
        },
        "data": {
            "old_ner": old_ner, "new_ner": new_ner,
            "old_search": old_search, "new_search": new_search,
            "old_final": old_final, "new_final": new_final,
            "new_ner_raw": new_ner_raw,
            "new_search_raw": new_search_raw,
            "new_final_raw": new_final_raw}
    }