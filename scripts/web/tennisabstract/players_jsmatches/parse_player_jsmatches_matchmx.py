from typing import (
    Dict,
    List,
)
import logging

def parse_player_jsmatches_matchmx(
    player_matchmx_list: List[List]
) -> List[Dict]:
    """
    Arguments:
    - player_matchmx_list: List of player matchmx data (as lists)

    Loops through list and returns list of player match dictionaries.
    """

    try:
        
        # set header list
        matchmx_header_list = [
            "date", "tourn", "surf", "level", "wl", "rank", "seed", "entry", "round", "score", "max",
            "opp", "orank", "oseed", "oentry", "ohand", "obday", "oht", "ocountry", "oactive", "time",
            "aces", "dfs", "pts", "firsts", "fwon", "swon", "games", "saved", "chances",
            "oaces", "odfs", "opts", "ofirsts", "ofwon", "oswon", "ogames", "osaved", "ochances", "obackhand",
            "chartlink", "pslink", "whserver", "matchid", "wh", "roundnum", "matchnum",
        ]
        

        # loop through matchmx list
        matchmx_data_list = []

        for matchmx in player_matchmx_list:

            # Skip if incomplete
            if len(matchmx) != len(matchmx_header_list):
                logging.info(f"Header count ({len(matchmx_header_list)}) does not match value count ({len(matchmx)}).")
                continue

            matchmx_dict = dict(zip(matchmx_header_list, matchmx))

            matchmx_data_list.append(matchmx_dict)

        return matchmx_data_list
    
    except Exception as e:
        logging.error(f"Error when parsing matchmx: {e}.")