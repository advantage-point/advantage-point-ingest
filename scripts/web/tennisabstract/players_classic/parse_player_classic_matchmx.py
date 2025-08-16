from typing import (
    List,
)
import logging

def parse_player_classic_matchmx(
    player_matchmx_list: List[List]
) -> str:
    """
    Arguments:
    - player_matchmx_list: List of player matchmx data (as lists)

    Loops through list and returns list of player match dictionaries.
    """

    try:
        
        # set header list: contains list of dicts that are header:index pairs
        # each matchmx record contains an extra '' element so this dict is needed to map the header to the correct element
        matchmx_header_dict = {
            "date": 0,
            "tourn": 1,
            "surf": 2,
            "level": 3,
            "wl": 4,
            "rank": 5,
            "seed": 6,
            "entry": 7,
            "round": 8,
            "score": 9,
            "max": 10,
            "opp": 11,
            "orank": 12,
            "oseed": 13,
            "oentry": 14,
            "ohand": 15,
            "obday": 16,
            "oht": 17,
            "ocountry": 18,
            "oactive": 19,
            "time": 20,
            "aces": 21,
            "dfs": 22,
            "pts": 23,
            "firsts": 24,
            "fwon": 25,
            "swon": 26,
            "games": 27,
            "saved": 28,
            "chances": 29,
            "oaces": 30,
            "odfs": 31,
            "opts": 32,
            "ofirsts": 33,
            "ofwon": 34,
            "oswon": 35,
            "ogames": 36,
            "osaved": 37,
            "ochances": 38,
            "obackhand": 39,
            "chartlink": 40,
            "pslink": 41,
            "whserver": 42,
            "matchid": 43,
            # 44: empty placeholder
            "wh": 45,
            "roundnum": 46,
            "matchnum": 47,
        }
        

        # loop through matchmx list
        matchmx_data_list = []

        for matchmx in player_matchmx_list:

            matchmx_dict = {
                header: matchmx[elem_index]
                for header, elem_index in matchmx_header_dict.items()
            }

            matchmx_data_list.append(matchmx_dict)

        return matchmx_data_list
    
    except Exception as e:
        logging.error(f"Error when parsing matchmx: {e}.")
