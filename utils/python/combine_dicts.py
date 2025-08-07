from typing import Dict

def combine_dicts(
    *dicts: Dict
) -> Dict:
    """
    Arguments:
    - *dicts: n number of dicts passed

    Combines dictionaries into one dict, overriding missing/null values.
    """
    combined_dict = {}
    
    for d in dicts:
        for key, value in d.items():
            # Check if the key already exists in the combined dictionary
            if key not in combined_dict:
                combined_dict[key] = value
            elif not combined_dict[key] and value:  # If the combined value is empty/null and the new value is not
                combined_dict[key] = value

    return combined_dict
