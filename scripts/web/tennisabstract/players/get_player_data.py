from scripts.web.tennisabstract.players.get_player_data_scraped import (
    get_player_jsmatches_data_scraped,
    get_player_jsmatches_career_data_scraped,
)
from typing import (
    Dict,
    List,
)
from utils.python.combine_list_of_dicts import combine_list_of_dicts
import logging

def get_player_data(
    player_url_list: List[Dict]
) -> List[Dict]:
    """
    Arguments:
    - player_url_list: List of player urls

    Create list of player data from list of player urls.
    """

    try:

        # loop through player urls
        player_data_list = []
        for i, player_dict in enumerate(player_url_list):

            player_name = player_dict['player_name']
            logging.info(f"({i+1}/{len(player_url_list)}) Getting player data for player: {player_name}.")

            
            # get data from player jsmatches scraping
            player_jsmatches_url = player_dict['player_jsmatches_url']
            player_jsmatches_scrape_dict = get_player_jsmatches_data_scraped(
                player_jsmatches_url=player_jsmatches_url,
                retries=3,
                delay=3
            )

            # get data frm player jsmatches career scraping
            player_jsmatches_career_url = player_dict['player_jsmatches_career_url']
            player_jsmatches_career_scrape_dict = get_player_jsmatches_career_data_scraped(
                player_jsmatches_career_url=player_jsmatches_career_url,
                retries=3,
                delay=3
            )

            # combine the 'matchmx' arrays if overlap
            if player_jsmatches_scrape_dict['matchmx'] and player_jsmatches_career_scrape_dict['morematchmx']:
                player_jsmatches_scrape_dict['matchmx'] = combine_list_of_dicts(
                    player_jsmatches_scrape_dict['matchmx'],
                    player_jsmatches_career_scrape_dict['morematchmx'],
                )

            # combine player scrape dicts into one
            player_scrape_dict = {
                **player_jsmatches_scrape_dict,
            }

            # continue with player data logic if data is returned from scraping
            if player_scrape_dict != {}:

                logging.info(f"Data found for player: {player_name}.")

                # combine player data
                player_data_dict = {
                    **player_dict,
                    **player_scrape_dict,
                }

                # append to list
                player_data_list.append(player_data_dict)

        return player_data_list

    except Exception as e:
        logging.error(f"Error when scraping player data for {player_name}: {e}.")
        return []