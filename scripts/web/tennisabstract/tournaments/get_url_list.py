from typing import (
    Dict,
    List,
)
from scripts.web.tennisabstract.matches.get_url_list import (
    main as get_match_url_list,
)
from scripts.web.tennisabstract.tournaments.create_tournament_url import (
    create_tournament_url,
)

def main():

    # here is where the function can be swapped out
    tournament_list = get_tournament_url_list_from_matches()
    return tournament_list


def get_tournament_url_list_from_matches() -> List[Dict]:
    """
    Returns list of tournament urls using the match url list.
    """

    # get match url list
    match_url_list = get_match_url_list()

    # initialize player dicts
    tournament_url_list = []
    tournament_urls_seen = set()

    # loop through match data
    for match_url_dict in match_url_list:

        # get tournament properties
        tournament_year = match_url_dict['match_date'][:4]
        tournament_gender = match_url_dict['match_gender']
        tournament_name = match_url_dict['match_tournament'].replace('_', ' ')

        # create tournament url
        tournament_url = create_tournament_url(
            tournament_year=tournament_year,
            tournament_gender=tournament_gender,
            tournament_name=tournament_name
        )

        # check if tournament url has already been added
        if tournament_url not in tournament_urls_seen:

            # construct and append dict
            tournament_url_dict = {
                'tournament_url': tournament_url,
                'tournament_year': tournament_year,
                'tournament_gender': tournament_gender,
                'tournament_name': tournament_name,
            }
            tournament_url_list.append(tournament_url_dict)

            # add to seen urls
            tournament_urls_seen.add(tournament_url)
        
    return tournament_url_list