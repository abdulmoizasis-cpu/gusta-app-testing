import re

def extract_keyword_set(data_list):
    """
    Processes a list of strings to extract a clean set of unique keywords.
    Correctly handles apostrophes like 's.
    """
    # This check ensures that if a non-list or None is passed, it returns an empty set gracefully.
    if not isinstance(data_list, list):
        return set()
    
    full_text = ' '.join(data_list).lower()
    text_no_possessive = re.sub(r"'s\b", "", full_text)
    cleaned_text = re.sub(r'[^a-z0-9\s]', '', text_no_possessive)
    keywords = set(filter(None, cleaned_text.split()))
    
    return keywords

def calculate_similarity(list1, list2):
    """
    Calculates similarity based on the presence of unique keywords in either list.
    Includes validation for None and list types.
    """

    if bool(list1) != bool(list2) :
        return True

    if list1 is None and list2 is None:
        return False
        
    if list1 is None or list2 is None:
        return True
        
    if not isinstance(list1, list) or not isinstance(list2, list):
        return False

    
    keywords1 = extract_keyword_set(list1)
    keywords2 = extract_keyword_set(list2)
    
    intersection = keywords1.intersection(keywords2)
    
    unique_1 = len(keywords1) - len(intersection)
    unique_2 = len(keywords2) - len(intersection)

    if unique_1 or unique_2:
        return True
    else: 
        return False
    
def remove_plural_pairs(list1, list2):
    def get_base(word):
        lowered_word = word.lower()
        if lowered_word.endswith("'s"):
            return lowered_word[:-2]
        if lowered_word.endswith('s'):
            return lowered_word[:-1]
        return lowered_word

    bases1 = {get_base(w) for w in list1 if isinstance(w, str)}
    bases2 = {get_base(w) for w in list2 if isinstance(w, str)}
    common_bases = bases1.intersection(bases2)

    new_list1 = [w for w in list1 if get_base(w) not in common_bases]
    new_list2 = [w for w in list2 if get_base(w) not in common_bases]
    return new_list1, new_list2
