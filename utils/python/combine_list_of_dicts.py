from typing import (
    Dict,
    List,
)
import json

def combine_list_of_dicts(*lists) -> List[Dict]:
    """
    Arguments:
    - *lists: any number of lists of objects

    Returns a deduped/combined list of objects.
    """

    combined = []
    for lst in lists:
        combined.extend(lst)
    
    seen = set()
    unique_dicts = []
    for d in combined:
        dict_str = json.dumps(d, sort_keys=True)
        if dict_str not in seen:
            seen.add(dict_str)
            unique_dicts.append(d)
    return unique_dicts
