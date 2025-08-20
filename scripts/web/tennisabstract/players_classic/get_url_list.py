from typing import (
    Dict,
    List,
)
from scripts.web.tennisabstract.matches.get_url_list import (
    main as get_match_url_list,
)
from scripts.web.tennisabstract.players_classic.create_player_classic_url import (
    create_player_classic_url,
)
from utils.python.combine_list_of_dicts import combine_list_of_dicts
from utils.web.scrape_javascript_var import scrape_javascript_var
from utils.web.make_request import make_request
import ast
import re

def main():

    # here is where the function can be swapped out
    player_list = get_player_classic_url_list_from_matches()
    return player_list

def get_player_classic_url_list() -> List[Dict]:
    """
    Returns list of player urls from source (url)
    """

    # retrieve url page
    player_list_url = 'https://www.tennisabstract.com/jsplayers/mwplayerlist.js'
    response = make_request(player_list_url)
    
    # retrieve list-like string
    player_list_val = scrape_javascript_var(
        content=response.text,
        var='playerlist'
    )
    # convert to list
    player_list = ast.literal_eval(player_list_val)

    # loop through each element and create url
    player_classic_url_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        player_classic_url_dict = {}

        player_classic_url_dict['player_name'] = name
        player_classic_url_dict['player_gender'] = gender

        # create url
        player_classic_url = create_player_classic_url(
            player_gender=gender,
            player_name=name
        )
        player_classic_url_dict['player_classic_url'] = player_classic_url

        player_classic_url_list.append(player_classic_url_dict)

    return player_classic_url_list

def get_player_classic_url_list_from_matches() -> List[Dict]:
    """
    Returns list of player urls using the match url list.
    """

    # get match url list
    match_url_list = get_match_url_list()

    # initialize player dicts
    player_classic_url_list = []
    player_classic_urls_seen = set()

    # loop through match data
    for match_url_dict in match_url_list:

        # loop through player data
        player_name_list = [
            match_url_dict['match_player_one'].replace('_', ' '),
            match_url_dict['match_player_two'].replace('_', ' ')
        ]
        for player_name in player_name_list:

            # get gender
            player_gender = match_url_dict['match_gender']

            # create player url
            player_classic_url = create_player_classic_url(
                player_name=player_name,
                player_gender=player_gender
            )

            # check if player_classic_url has already been added
            if player_classic_url not in player_classic_urls_seen:

                # construct and append dict
                player_classic_url_dict = {
                    'player_classic_url': player_classic_url,
                    'player_name': player_name,
                    'player_gender': player_gender,
                }
                player_classic_url_list.append(player_classic_url_dict)

                # add to seen urls
                player_classic_urls_seen.add(player_classic_url)
    
    return player_classic_url_list