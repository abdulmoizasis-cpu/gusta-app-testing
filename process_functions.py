from helpers import *
from streams import *

def process_convo_row(api_query, index, user_query, old_ner, old_ner_intent, use_agent_stream=False, stop_event=None):
        new_ner_intent, new_ner_search_fields, new_chain_field_values, new_ner_date_filter= "", "", "", ""
        new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp = "", "", "", "", "", "", ""
        new_ner_leaf_entities = ""

        if use_agent_stream:
            new_ner_raw, new_final_raw, new_search_raw, new_time_stamp, latency = get_api_results_from_agent_stream(api_query, stop_event)
        else:
            new_ner_raw, new_final_raw, new_search_raw, new_time_stamp, latency = get_api_results_from_conversational_stream(api_query, stop_event)

        new_ner = new_ner_raw
        if new_ner and isinstance(new_ner, dict):
            new_ner_intent = new_ner.get("intent", "")
            new_ner_search_fields = new_ner.get("search_fields", "")
            new_ner_leaf_entities = new_ner.get("leaf_entities", "")

            if new_ner_search_fields:
                new_ner_date_filter =[field.get("date_filter", "").get("value", "") for field in new_ner_search_fields if isinstance(field, dict)]
                new_ner_search_fields = [field for field in new_ner_search_fields if not isinstance(field, dict)]
        else :
            new_ner = "The fresh API call returned no results for this row"

        new_search = new_search_raw
        if new_search and "feedback_message" in new_search:
            new_search.pop("feedback_message")

        if (new_ner_intent != old_ner_intent and old_ner_intent == ["search_list"]) :
            new_search = "Change in intent detected, no corresponding search chain output exists !"
            
        if new_search and isinstance(new_search, str) and new_search != "{}" and not new_search.startswith("Conversational") and not new_search.startswith("API") :
            try:
                new_search = json.loads(new_search)
                if not isinstance(new_search, str):
                    new_chain_search_fields = new_search.get("search_fields", "")
                    new_chain_field_values= [item.get("field_value", "") for item in new_chain_search_fields if item.get("field_type", "") != "date"]
            except json.JSONDecodeError:
                new_search = {}
            
        new_final = new_final_raw

        return new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values, latency

def process_single_row(api_query, index, user_query, old_ner, old_ner_intent, use_agent_stream=False,stop_event=None):
    new_ner_intent, new_ner_search_fields, new_chain_field_values, new_ner_date_filter= "", "", "", ""
    new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp = "", "", "", "", "", "", ""
    new_ner_leaf_entities = ""

    if use_agent_stream:
        new_ner_raw, new_final_raw, new_search_raw, new_time_stamp, latency = get_api_results_from_agent_stream(api_query, stop_event)
    else:
        new_ner_raw, new_final_raw, new_search_raw, new_time_stamp, latency = get_api_results_from_stream(api_query, stop_event)    

    if use_agent_stream:
        new_ner = new_ner_raw 
    else:
        new_ner = parse_csv_text_to_json(new_ner_raw) 
        
    if new_ner and isinstance(new_ner, dict):
        new_ner_intent = new_ner.get("intent", "")
        new_ner_search_fields = new_ner.get("search_fields", "")
        new_ner_leaf_entities = new_ner.get("leaf_entities", "")

        if new_ner_search_fields:
            new_ner_date_filter =[field.get("date_filter", "").get("value", "") for field in new_ner_search_fields if isinstance(field, dict)]
            new_ner_search_fields = [field for field in new_ner_search_fields if not isinstance(field, dict)]
    else :
        new_ner = "The fresh API call returned no results for this row"

    new_search = convert_yaml_text_to_json(new_search_raw)
    if new_search and "feedback_message" in new_search:
        new_search.pop("feedback_message")

    if (new_ner_intent != old_ner_intent and old_ner_intent == ["search_list"]) :
        new_search = "Change in intent detected, no corresponding search chain output exists !"
        
    if isinstance(new_search, dict) :
        new_chain_search_fields = new_search.get("search_fields", "")
        new_chain_field_values= [item.get("field_value", "") for item in new_chain_search_fields if item.get("field_type", "") != "date"]
        
    if isinstance(new_final_raw, dict) :
       new_final = new_final_raw['url']
    else :
        new_final = extract_url(new_final_raw)

    return new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values, latency

def fill_empty_row_group(group_df):
    """
    Processes a group of rows with empty ner_output.
    Makes one API call and updates all rows in the group with the result.
    """
    base_row = group_df.iloc[0]
    user_query = base_row.get('user_query', "")
    if not user_query:
        return

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

    new_ner_raw, new_final_raw, new_search_raw, time_stamp = "", "", "", ""
    if query_type == "conversational":
        new_ner_raw, new_final_raw, new_search_raw, time_stamp, _ = get_api_results_from_conversational_stream(api_query)
    else:
        new_ner_raw, new_final_raw, new_search_raw, time_stamp, _ = get_api_results_from_stream(api_query)

    updates = {
        'ner_output': json.dumps(new_ner_raw) if isinstance(new_ner_raw, (dict, list)) else new_ner_raw,
        'search_list_chain_output': json.dumps(new_search_raw) if isinstance(new_search_raw, (dict, list)) else new_search_raw,
        'final_output': json.dumps(new_final_raw) if isinstance(new_final_raw, (dict, list)) else new_final_raw,
        'query_type': query_type,
        'time_stamp':time_stamp
    }

    current_ids = group_df['id'].tolist()
    update_database_record(current_ids, updates)

    
