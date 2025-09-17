from helpers import *
from streams import *

def process_convo_row(api_query, index, user_query, old_ner, old_ner_intent) :
        new_ner_intent, new_ner_search_fields, new_chain_field_values, new_ner_date_filter= "", "", "", ""
        new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp = "", "", "", "", "", "", ""
        new_ner_leaf_entities = ""

        new_ner_raw, new_final_raw, new_search_raw, new_time_stamp = get_api_results_from_conversational_stream(api_query)
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
            new_search = json.loads(new_search)
            if not isinstance(new_search, str):
                new_chain_search_fields = new_search.get("search_fields", "")
                new_chain_field_values= [item.get("field_value", "") for item in new_chain_search_fields if item.get("field_type", "") != "date"]
            
        new_final = new_final_raw

        return new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values

def process_single_row(api_query, index, user_query, old_ner, old_ner_intent) :
    new_ner_intent, new_ner_search_fields, new_chain_field_values, new_ner_date_filter= "", "", "", ""
    new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp = "", "", "", "", "", "", ""
    new_ner_leaf_entities = ""
        
    new_ner_raw, new_final_raw, new_search_raw, new_time_stamp = get_api_results_from_stream(api_query)
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

    return new_ner_raw, new_search_raw, new_final_raw , new_ner, new_search, new_final, new_time_stamp, new_ner_intent, new_ner_search_fields, new_ner_leaf_entities, new_ner_date_filter, new_chain_field_values


    
