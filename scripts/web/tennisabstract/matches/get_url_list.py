from bs4 import BeautifulSoup
from scripts.web.tennisabstract.matches.create_match_url import create_match_url
from typing import (
    Dict,
    List,
)
from utils.web.make_request import make_request

def main() -> List[Dict]:
    """
    Returns list of match urls from source (url)
    """

    # retrieve url page source
    match_list_url = 'https://www.tennisabstract.com/charting/'
    response = make_request(url=match_list_url)

    # parse page source
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # links are hrefs in last <p>
    p_tag_match = soup.find_all('p')[-1]

    # construct list of dicts
    match_url_list = []
    for a in p_tag_match.find_all('a', href=True):

        # initialize dict
        match_url_dict = {}

        match_url_suffix = a['href']
        
        # create match url
        match_url = create_match_url(match_url_suffix=match_url_suffix)
        match_url_dict['match_url'] = match_url

        # parse url
        match_url_suffix_parsed_list = match_url_suffix.replace('.html', '').split('-')
        match_date = match_url_suffix_parsed_list[0]
        match_url_dict['match_date'] = match_date
        match_gender = match_url_suffix_parsed_list[1]
        match_url_dict['match_gender'] = match_gender
        match_tournament = match_url_suffix_parsed_list[2]
        match_url_dict['match_tournament'] = match_tournament
        match_round = match_url_suffix_parsed_list[3]
        match_url_dict['match_round'] = match_round
        match_player_one = match_url_suffix_parsed_list[4]
        match_url_dict['match_player_one'] = match_player_one
        match_player_two = match_url_suffix_parsed_list[5]
        match_url_dict['match_player_two'] = match_player_two

        match_url_list.append(match_url_dict)

    return match_url_list