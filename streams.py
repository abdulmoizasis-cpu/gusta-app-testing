from helpers import *
import json, datetime, time, requests

def get_api_results_from_conversational_stream(query_text):
    history = []
    lines = [line.strip() for line in query_text.split('\n') if line.strip()]
    final_response_data = None
    
    inverse_map = create_inverse_field_map(reconstructed_search_mapping)
    start_time= time.time()
    max_retries = 5
    trial = 1
    last_error = "API call returned no error"

    for i, line in enumerate(lines):
        payload = {"query": line, "conversation_history": history}
        for attempt in range(max_retries):
            trial += 1
            try:
                response = requests.post("https://aitest.ebalina.com/invoke", json=payload, timeout=90)
                response.raise_for_status()
                data = response.json()   
                if "ner_output" in data:
                    history.append({"user": line, "ai": data["ner_output"]})
                if i == len(lines) - 1:
                    final_response_data = data
                break 
            except requests.exceptions.RequestException as e:
                last_error = e
                time.sleep(1) 
        else:
            error_message = f"Retried {max_retries} times but API call failed for line: '{line}'."
            if last_error:
                error_message += f"\n Error: {last_error}"
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return error_message, error_message, error_message, current_time, 0
    end_time = time.time()
    latency = end_time- start_time

    if final_response_data:
        ner_output_raw = final_response_data.get("ner_output", "")
        final_output_raw = final_response_data.get("output", {})
        search_output_raw = ""
        
        url_to_process = final_output_raw.get("url")
        
        if url_to_process:
            ner_as_json = convert_yaml_text_to_json(ner_output_raw)
            search_list_chain_output = reverse_engineer_search_output(url_to_process, inverse_map)
            search_output_raw = json.dumps(search_list_chain_output)
        else:
            search_output_raw = "{}" 

        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return ner_as_json, url_to_process, search_output_raw, current_time, latency
    
    error_message = "Conversational query processed, but no final response was captured."
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return error_message, error_message, error_message, current_time, latency


def get_api_results_from_stream(query_text):
    max_retries = 5
    trial = 1
    last_error = "API call returned no error"
    payload = {"query": query_text, "k": 5}
    for attempt in range(max_retries) : 
        trial += 1
        start_time = time.time()
        try:
            response = requests.post("https://aitest.ebalina.com/stream", json=payload, stream=True, timeout=90)
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
            
            ner_output, final_output, search_list_chain_output = None, None, None

            if full_response_data : 
                for item in full_response_data:
                    if item.get("log_title") == "NER Succeded":
                        content = item.get("content")
                        ner_output = json.dumps(content) if isinstance(content, (dict, list)) else str(content)
                    if item.get("log_title") == "Search List Result":
                        content = item.get("content")
                        search_list_chain_output = json.dumps(content) if isinstance(content, (dict, list)) else str(content)
                if ner_output == None :
                    continue
                end_time = time.time()
                latency = end_time - start_time
                time_stamp = full_response_data[0].get("timestamp")
                time_stamp = datetime.datetime.fromtimestamp(time_stamp).strftime("%Y-%m-%d %H:%M:%S")
                final_output = full_response_data[-1].get("output", "")
                return ner_output, final_output, search_list_chain_output, time_stamp, latency
        except requests.exceptions.RequestException as e:
            last_error = e
            time.sleep(1)

    error_message = "Retried 5 times but api call returned no results"
    if last_error:
        error_message += f"\n Error : {last_error}"
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return error_message, error_message, error_message, current_time , 0