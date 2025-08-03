from typing import (
    Dict,
    List,
)
from scripts.web.tennisabstract.players.create_player_url import create_player_url
from scripts.web.tennisabstract.players.create_player_classic_url import create_player_classic_url
from scripts.web.tennisabstract.players.create_player_jsmatches_url import create_player_jsmatches_url
from utils.web.scrape_javascript_var import scrape_javascript_var
from utils.web.make_request import make_request
import ast
import re

def get_player_url_list() -> List[Dict]:
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
    player_url_list = []
    for player in player_list:

        # each element in list is of format: (<gender>) <name>)
        regex_pattern = r'(?P<gender>\((.*?)\))\s*(?P<name>.*)'
        regex_match = re.search(regex_pattern, player)
        gender = regex_match.group('gender').strip('()')
        name = regex_match.group('name')

        player_url_dict = {}

        player_url_dict['player_name'] = name
        player_url_dict['player_gender'] = gender

        # create url
        player_url = create_player_url(
            player_gender=gender,
            player_name=name
        )
        player_url_dict['player_url'] = player_url

        # create player classic url
        player_classic_url = create_player_classic_url(
            player_name=name,
            player_gender=gender
        )
        player_url_dict['player_classic_url'] = player_classic_url

        # create player jsmatches url
        player_jsmatches_url = create_player_jsmatches_url(
            player_name=name
        )
        player_url_dict['player_jsmatches_url'] = player_jsmatches_url

        player_url_list.append(player_url_dict)

    return player_url_list