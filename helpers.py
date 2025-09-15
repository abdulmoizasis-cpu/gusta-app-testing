import json, ast, yaml, urllib, re, difflib, db_utils, streamlit as st
from st_copy_to_clipboard import st_copy_to_clipboard
from keywords_check import *
from streamlit_extras.stylable_container import stylable_container

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
    
    cleaned_text = yaml_text.replace("```yaml", "").replace("```", "").strip() # This line is the only change
    
    try:
        parsed_data = yaml.safe_load(cleaned_text)
        return parsed_data if isinstance(parsed_data, dict) else {"parsed_content": parsed_data}
    except yaml.YAMLError:
        return {"raw_unparseable_text": cleaned_text}

def create_inverse_field_map(mapping):
    """Creates a reverse-lookup map {name: {module: numeric_id}}."""
    inverse_map = {}
    for field_id, search_modules in mapping.items():
        for search_name, new_field_name in search_modules.items():
            if new_field_name not in inverse_map:
                inverse_map[new_field_name] = {}
            inverse_map[new_field_name][search_name] = field_id
    return inverse_map

def is_date_value(value):
    """Checks if a string value matches common date formats."""
    # Matches formats like ">=2024-01-01", "2024", "<=2023-12-31"
    date_pattern = r"^(>=|<=|>|<)?\d{4}(-\d{2}(-\d{2})?)?$"
    return re.match(date_pattern, value) is not None

def parse_search_url(url):
    """Parses the URL fragment to extract the search module and fields."""
    try:
        fragment = url.split('#')[1]
    except IndexError:
        return None, []

    params = fragment.split('&')
    search_name = ""
    fields_dict = {}

    for param in params:
        if '=' in param: 
            key, value = param.split('=', 1)
        else:
            key, value = param, "" 

        key = urllib.parse.unquote(key)
        value = urllib.parse.unquote(value)

        if key == "search[name]":
            search_name = value
        
        match = re.match(r"search\[fields\]\[(\d+)\]\[(name|value)\]", key)
        if match:
            index, part = match.groups()
            if index not in fields_dict:
                fields_dict[index] = {}
            fields_dict[index][part] = value
            
    return search_name, [v for k, v in sorted(fields_dict.items())]

def reverse_engineer_search_output(api_url, inverse_map):
    """
    Reconstructs the search_list_chain_output JSON from the final URL.
    """
    search_name, transformed_fields = parse_search_url(api_url)

    if not search_name:
        return ""

    reconstructed_fields = []
    for field in transformed_fields:
        field_name = field.get("name")
        field_value = field.get("value")

        if is_date_value(field_value):
            # Format as a date field
            reconstructed_fields.append({
                "field_name": field_name,
                "field_type": "date",
                "field_value": field_value
            })
        else:
            # Format as a standard field with a numeric ID
            numeric_id = inverse_map.get(field_name, {}).get(search_name, "N/A")
            for value in field_value.split('|'):
                reconstructed_fields.append({
                    "field_id": "N/A",
                    "field_name": field_name,
                    "field_value": value
                })

    return {
        "search_fields": reconstructed_fields,
        "search_name": search_name
    }
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
    insert_style = f"background-color: #99ff99; color: #000; {style}"
    delete_style = f"background-color: #ff9999; color: #000; {style}"

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
    query = f"UPDATE `test_results` SET {set_clauses} WHERE `id` = :id"
    
    params = updates.copy()
    params['id'] = record_id
    
    db_utils.execute_query("llm", query, params)

def display_diff(title, old_data, new_data, row_id, column_name, new_raw_data, buttons_enabled=False):
    st.subheader(title)

    if title == "NER Output Difference":
        parsed_old_data = parse_csv_text_to_json(old_data) if isinstance(old_data, str) else old_data
        
        if isinstance(new_data, str) and (new_data.startswith("Conversational") or new_data.startswith("Retried")):
            parsed_new_data = new_data
        else:
            parsed_new_data = parse_csv_text_to_json(new_data) if isinstance(new_data, str) else new_data
        
        old_text = json.dumps(parsed_old_data, indent=4, sort_keys=True) if isinstance(parsed_old_data, (dict, list)) else str(parsed_old_data or "")
        new_text = json.dumps(parsed_new_data, indent=4, sort_keys=True) if isinstance(parsed_new_data, (dict, list)) else str(parsed_new_data or "")

    else:
        old_text = json.dumps(old_data, indent=4, sort_keys=True) if isinstance(old_data, (dict, list)) else str(old_data or "")
        new_text = json.dumps(new_data, indent=4, sort_keys=True) if isinstance(new_data, (dict, list)) else str(new_data or "")

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

def display_result_expander(result, buttons_enabled=False):
    if not result:
        return
    
    st.markdown("""
    <style>
    /* Make ONLY the copy-to-clipboard button neutral on hover */
    div[data-testid="stCopyToClipboard"] button[data-baseweb="button"]:hover {
        background-color: #f0f2f6 !important;
        color: #31333f !important;
        border-color: #f0f2f6 !important;
        box-shadow: none !important;
    }
    </style>
    """, unsafe_allow_html=True)


    if result.get('status') == 'deleted_duplicate':
        st.error(f"Row ID {result['id']}: {result['error']}")
        return

    if result.get('failed'):
        with st.expander(f"🚨 Row ID: {result['id']}"):
            if result.get('error'):
                st.error(f"Could not process row: {result['error']}")
                return

            st.text_area("User Query:", result['user_query'], height=30, key=f"query_{result['id']}")
            
            action_cols = st.columns(2)
            with action_cols[0]:
                from streamlit_extras.stylable_container import stylable_container
                with stylable_container(
                    key=f"neutral_copy_{result['id']}",
                    css_styles="""
                    {
                      /* scope the primary color only for descendants of this container */
                      --primary-color: #f0f2f6;
                      --text-color: #31333f;
                    }
                    /* Streamlit/BaseWeb button inside the copy widget */
                    div[data-testid="stCopyToClipboard"] button[data-baseweb="button"] {
                        /* optional: ensure default state is not tinted */
                        box-shadow: none !important;
                        border-color: transparent !important;
                    }
                    div[data-testid="stCopyToClipboard"] button[data-baseweb="button"]:hover {
                        background-color: #f0f2f6 !important;
                        color: #31333f !important;
                        border-color: #f0f2f6 !important;
                        box-shadow: none !important;
                    }
                    div[data-testid="stCopyToClipboard"] button[data-baseweb="button"]:focus,
                    div[data-testid="stCopyToClipboard"] button[data-baseweb="button"]:focus-visible {
                        outline: none !important;
                        box-shadow: none !important;
                        border-color: #f0f2f6 !important;
                    }
                    """
                ):
                    st_copy_to_clipboard(result['user_query'], "Copy Query", key=f"copy_{result['id']}")

            if buttons_enabled:
                with action_cols[1]:
                    if st.button("Replace All Cells", key=f"replace_all_{result['id']}"):
                        updates = {
                            'ner_output': result['data']['new_ner_raw'],
                            'search_list_chain_output': result['data']['new_search_raw'],
                            'final_output': result['data']['new_final_raw']
                        }
                        update_database_record(result['id'], updates)
                        st.toast(f"All outputs for row `{result['id']}` replaced.", icon="🔄")
                
            if result["failures"]["ner"]:
                display_diff("NER Output Difference", result["data"]["old_ner"], result["data"]["new_ner_raw"], result['id'], 'ner_output', result['data']['new_ner_raw'], buttons_enabled)
                st.divider()

            if not isinstance(result["data"]["new_ner_raw"], str) or not result["data"]["new_ner_raw"].startswith("Retried"):
                if result["failures"]["search"]:
                    display_diff("Search Output Difference", result["data"]["old_search"], result["data"]["new_search"], result['id'], 'search_list_chain_output', result['data']['new_search_raw'], buttons_enabled)
                    st.divider()

                if result["failures"]["final"]:
                    display_diff("Final Output Difference", result["data"]["old_final"], result["data"]["new_final"], result['id'], 'final_output', result['data']['new_final_raw'], buttons_enabled)


reconstructed_search_mapping = {
    "highly_potent": {"compound": "highly_potent", "molecule": "highly_potent"},
    "therapeutic_category_s": {
        "compound": "therapeutic_category_s",
        "molecule": "therapeutic_category_s",
        "news": "therapeutic_category_s",
        "deal": "therapeutic_category_s",
        "venture": "therapeutic_category_s",
        "technology": "therapeutic_category_s",
        "discovery_technology": "pp_therapeutic_category",
        "company": "therapeutic_category_s",
        "howsupplied": "therapeutic_category_s",
        "howsupplied_injectable": "therapeutic_category_s",
    },
    "highest_phase_compound": {
        "compound": "highest_phase_compound",
        "molecule": "highest_phase",
        "news": "highest_phase",
        "deal": "highest_phase",
        "venture": "highest_phase",
        "technology": "development_stage",
        "company": "highest_phase_compound",
        "discovery_technology": "highest_phase",
        "howsupplied": "highest_phase_compound",
        "howsupplied_injectable": "highest_phase_compound",
    },
    "phase": {"compound": "phase"},
    "activity": {
        "compound": "activity",
        "technology": "activity",
        "company": "company_is_active",
        "howsupplied": "activity",
        "howsupplied_injectable": "activity",
    },
    "product_name_s": {
        "compound": "product_name_s",
        "news": "compound_name_s",
        "deal": "product_name_s",
        "discovery_technology": "compound_name_s",
        "paragraph_iv": "innovator_compound_s",
        "howsupplied": "product_name_s",
        "howsupplied_injectable": "product_name_s",
    },
    "generic_or_innovator": {
        "compound": "generic_or_innovator",
        "molecule": "generic_or_innovator",
        "news": "generic_or_innovator",
        "deal": "generic_or_innovator",
        "venture": "generic_or_innovator",
        "technology": "generic_or_innovator",
        "company": "generic_or_innovator",
        "howsupplied": "generic_or_innovator",
        "howsupplied_injectable": "generic_or_innovator",
    },
    "company_name_s": {
        "compound": "company_name_s",
        "molecule": "company_name_s",
        "news": "company_name_s",
        "deal": "company_name_s",
        "venture": "company_name_s",
        "technology": "company_name_s",
        "discovery_technology": "company_name_s",
        "company": "company_name_s",
        "howsupplied": "company_name_s",
        "howsupplied_injectable": "company_name_s",
    },
    "molecule_name_s": {
        "compound": "molecule_name_s",
        "molecule": "molecule_name_s",
        "news": "molecule_name_s",
        "deal": "molecule_name_s",
        "venture": "molecule_name_s",
        "technology": "molecule_name_s",
        "company": "molecule_name_s",
        "paragraph_iv": "innovator_molecule",
        "howsupplied": "molecule_name_s",
        "howsupplied_injectable": "molecule_name_s",
    },
    "molecule_api_group": {
        "compound": "molecule_api_group",
        "molecule": "molecule_api_group",
        "news": "molecule_api_group",
        "deal": "molecule_api_group",
        "venture": "molecule_api_group",
        "technology": "molecule_api_group",
        "company": "molecule_api_group",
        "discovery_technology": "molecule_api_group",
        "howsupplied": "molecule_api_group",
        "howsupplied_injectable": "molecule_api_group",
    },
    "conjugate_molecule_types": {
        "compound": "conjugate_molecule_types",
        "molecule": "conjugate_molecule_types",
        "deal": "conjugate_molecule_types",
        "news": "conjugate_molecule_types",
        "venture": "conjugate_molecule_types",
        "technology": "technology_molecule_types",
        "discovery_technology": "disco_molecule_types",
        "company": "conjugate_molecule_types",
        "howsupplied": "molecule_type",
        "howsupplied_injectable": "conjugate_molecule_types",
    },
    "mechanism_type_s": {
        "compound": "mechanism_type_s",
        "molecule": "mechanism_type_s",
        "venture": "mechanism_type",
        "deal": "mechanism_type_s",
        "company": "mechanism_type_s",
        "howsupplied": "mechanism_type_s",
        "howsupplied_injectable": "mechanism_type_s",
    },
    "route_branch_s": {
        "compound": "route_branch_s",
        "molecule": "route_branch_injection",
        "news": "route_branch_injection",
        "deal": "route_branch_injection",
        "venture": "route_branch_injection",
        "technology": "route_technology_injection",
        "company": "route_s",
        "discovery_technology": "route_branch",
        "howsupplied": "route_s",
        "howsupplied_injectable": "route_branch_s",
    },
    "drug_delivery_branch_s": {
        "compound": "drug_delivery_branch_s",
        "molecule": "drug_delivery_branch_s",
        "news": "drug_delivery_branch",
        "deal": "drug_delivery_branch_s",
        "venture": "drug_delivery_branch_s",
        "technology": "drug_delivery_branch_compound_s",
        "company": "drug_delivery_branch_s",
        "howsupplied": "drug_delivery_branch_s",
        "howsupplied_injectable": "drug_delivery_branch_s",
    },
    "antibody_type": {
        "compound": "antibody_type",
        "molecule": "antibody_type",
        "howsupplied": "antibody_type",
        "howsupplied_injectable": "antibody_type",
    },
    "antibody_source": {"compound": "antibody_source", "molecule": "antibody_source"},
    "antibody_class": {"compound": "antibody_class", "molecule": "antibody_class"},
    "antibody_fragment": {
        "compound": "antibody_fragment",
        "molecule": "antibody_fragment",
    },
    "bispecific_antibody": {
        "compound": "bispecific_antibody",
        "molecule": "bispecific_antibody",
    },
    "is_prodrug": {"compound": "is_prodrug", "molecule": "is_prodrug"},
    "fusion_protein": {"compound": "fusion_protein", "molecule": "fusion_protein"},
    "cell_source": {"compound": "cell_source", "molecule": "cell_source"},
    "release_profile": {"compound": "release_profile", "molecule": "release_profile"},
    "expression_organism": {"molecule": "expression_organism"},
    "target_type": {"compound": "target_type", "molecule": "target_type"},
    "target_name_s": {"compound": "target_name_s", "molecule": "target_name_s"},
    "immunooncology": {"compound": "immunooncology"},
    "vaccine_type": {"compound": "vaccine_type", "deal": "vaccine_type"},
    "vaccine_or_adjuvant": {
        "compound": "vaccine_or_adjuvant",
        "news": "vaccine_or_adjuvant",
        "deal": "vaccine_or_adjuvant",
        "technology": "vaccine_or_adjuvant",
        "company": "vaccine_or_adjuvant",
        "howsupplied_injectable": "vaccine_or_adjuvant",
    },
    "charged_molecule": {
        "compound": "charged_molecule",
        "molecule": "charged_molecule",
    },
    "water_solubility": {
        "compound": "water_solubility",
        "molecule": "water_solubility",
        "howsupplied": "water_solubility",
        "howsupplied_injectable": "water_solubility",
    },
    "estimated_water_solubility": {
        "compound": "estimated_water_solubility",
        "molecule": "estimated_water_solubility",
    },
    "BCS_classification_s": {
        "compound": "BCS_classification_s",
        "molecule": "BCS_classification",
        "howsupplied": "bcs_classification_s",
    },
    "chiral_form": {"molecule": "chiral_form"},
    "logp_reported": {"compound": "logP", "molecule": "logP"},
    "melting_point": {"compound": "melting_point_all", "molecule": "melting_point"},
    "pka_field": {"molecule": "pKa"},
    "molecular_weight": {"molecule": "molecular_weight"},
    "elimination_pathway": {"molecule": "elimination_pathway"},
    "is_the_metabolite_active": {"molecule": "is_the_metabolite_active"},
    "plasma_protein_binding": {"molecule": "plasma_protein_binding"},
    "volume_of_distribution": {"molecule": "volume_of_distribution"},
    "oral_bioavailability": {"molecule": "oral_bioavailability_percent"},
    "clearance": {"molecule": "clearance"},
    "elimination": {"molecule": "elimination_half_life"},
    "food_effect": {"compound": "food_effect", "molecule": "food_effect"},
    "p_gp_effect": {"molecule": "p_gp_effect"},
    "injection_site": {
        "compound": "injection_site",
        "howsupplied_injectable": "injection_site",
        "technology": "injection_site",
    },
    "dose_per_admin": {
        "howsupplied": "dose_per_admin",
        "howsupplied_injectable": "dose_per_admin",
    },
    "daily_dose": {"howsupplied": "daily_dose", "howsupplied_injectable": "daily_dose"},
    "injection_volume": {
        "howsupplied": "injection_volume",
        "howsupplied_injectable": "injection_volume",
    },
    "form_name_basic": {
        "compound": "form_name_s",
        "molecule": "molecule_dosage_form_s",
        "news": "form_name_s",
        "deal": "dosage_form",
        "venture": "dosage_form",
        "technology": "dosage_form",
        "company": "form_name_s",
        "discovery_technology": "dosage_form",
        "howsupplied": "form_name_s",
        "howsupplied_injectable": "form_name_s",
    },
    "dd_device_category": {
        "compound": "dd_device_category",
        "news": "dd_device",
        "technology": "dd_device_category",
        "howsupplied": "dd_device_category",
        "howsupplied_injectable": "dd_device_category",
    },
    "excipient_name_s": {
        "howsupplied": "excipient_name_s",
        "howsupplied_injectable": "excipient_name_s",
    },
    "preservative_free": {
        "howsupplied": "preservative_free",
        "howsupplied_injectable": "preservative_free",
    },
    "light_sensitive": {
        "howsupplied": "light_sensitive",
        "howsupplied_injectable": "light_sensitive",
    },
    "storage_temp": {
        "howsupplied": "storage_temp",
        "howsupplied_injectable": "storage_temp",
    },
    "name_s": {
        "compound": "technology_s",
        "news": "technology_name",
        "deal": "technology_s",
        "technology": "name_s",
        "howsupplied": "technology_s",
        "howsupplied_injectable": "technology_s",
    },
    "name": {"discovery_technology": "name"},
    "ingredient_volume": {"howsupplied_injectable": "ingredient_volume"},
    "pH": {"howsupplied": "pH", "howsupplied_injectable": "pH"},
    "color": {"howsupplied": "color"},
    "tablet_capsule_shape": {"howsupplied": "tablet_capsule_shape"},
    "tablet_coating": {"howsupplied": "tablet_coating"},
    "is_minitablet": {"howsupplied": "is_minitablet"},
    "length": {"howsupplied": "length"},
    "glass_type": {"howsupplied_injectable": "glass_type"},
    "package_color": {"howsupplied_injectable": "package_color"},
    "supplied_as_kit": {"howsupplied_injectable": "supplied_as_kit"},
    "primary_packaging": {"howsupplied": "primary_packaging"},
    "is_505_2_b": {"compound": "is_505_2_b"},
    "is_orphan": {
        "compound": "is_orphan",
        "howsupplied": "is_orphan",
        "howsupplied_injectable": "is_orphan",
    },
    "earliest_approval_date": {"compound": "earliest_approval_date"},
    "usa_earliest_approval_date": {"compound": "usa_earliest_approval_date"},
    "europe_earliest_approval_date": {"compound": "europe_earliest_approval_date"},
    "device_approval_date_s": {"technology": "device_approval_date_s"},
    "owner_company_private_public": {"company": "company_private_public"},
    "owner_company_business_model": {
        "news": "business_model",
        "deal": "type",
        "company": "company_business_model_exact",
    },
    "owner_company_major_business_model": {"company": "major_business_model"},
    "company_city": {"company": "company_city"},
    "company_territory": {
        "company": "company_territory",
        "compound": "owner_company_territory",
    },
    "owner_company_US_state": {"company": "company_US_state"},
    "company_year_founded": {"company": "company_year_founded"},
    "news_branch": {"news": "news_type", "deal": "news_branch"},
    "amendment_termination": {"deal": "amendment_termination"},
    "pharma_services_type": {"deal": "pharma_services_type"},
    "royalty_type": {"deal": "royalty_type"},
    "royalty_digit": {"deal": "royalty_digit"},
    "is_deal_cancelled": {"deal": "is_deal_cancelled"},
    "deal_date": {"deal": "date"},
    "news_date": {"news": "date"},
    "security_type": {"venture": "security_type"},
    "venture_type": {"venture": "venture_type"},
}