from typing import (
    Dict,
    List,
)

def parse_player_matchmx(
    player_matchmx_list: List[List]
) -> List[Dict]:
    """
    Arguments:
    - player_matchmx_list: List of player matchmx data (as lists)

    Loops through list and returns list of player match dictionaries.
    """

    # set header list
    matchmx_header_list = [
        "date","tourn","surf","level","wl","rank","seed","entry","round",
        "score","max","opp","orank","oseed","oentry","ohand","obday",
        "oht","ocountry","oactive","time","aces","dfs","pts","firsts","fwon",
        "swon",'games',"saved","chances","oaces","odfs","opts","ofirsts",
        "ofwon","oswon",'ogames',"osaved","ochances","obackhand","chartlink",
        "pslink","whserver","matchid",
    ]

    # loop through matchmx list
    matchmx_data_list = []
    for matchmx in player_matchmx_list:
        
        # initialize dict
        matchmx_dict = {}

        # loop through each matchmx record
        for i, matchmx_elem in enumerate(matchmx):
            
            # set header-value pairs
            matchmx_header = matchmx_header_list[i]
            matchmx_dict[matchmx_header] = matchmx_elem
        
        # append to list
        matchmx_data_list.append(matchmx_dict)

    return matchmx_data_list