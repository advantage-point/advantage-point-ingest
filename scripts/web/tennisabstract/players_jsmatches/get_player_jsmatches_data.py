from scripts.web.tennisabstract.players.get_player_data_scraped import (
    get_player_jsmatches_data_scraped,
    get_player_jsmatches_career_data_scraped,
)
from typing import (
    Dict,
    List,
)
from utils.python.combine_dicts import combine_dicts
from utils.python.combine_list_of_dicts import combine_list_of_dicts
import logging

def get_player_jsmatches_data(
    player_url_list: List[Dict]
) -> List[Dict]:
    """
    Arguments:
    - player_url_list: List of player urls

    Create list of player data from list of player urls.
    """

    # set logging config
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
    )

    try:

        # loop through player urls
        player_data_list = []
        for i, player_dict in enumerate(player_url_list):

            pass

        #     # if possible, get fullname to construct 'jsmatches' urls -> get 'jsmatches' data
        #     try:
        #         player_fullname = player_url_scrape_dict['fullname'] or player_classic_url_scrape_dict['fullname'] or player_name
        #         player_fullname = player_fullname.replace("'", "")
        #         if player_fullname:

        #             # get data from player jsmatches scraping
        #             try:

        #                 player_jsmatches_url = create_player_jsmatches_url(
        #                     player_name=player_fullname
        #                 )
        #                 player_dict['player_jsmatches_url'] = player_jsmatches_url
        #                 logging.info(f"Getting player jsmatches data from {player_jsmatches_url}.")
        #                 player_jsmatches_url_scrape_dict = get_player_jsmatches_data_scraped(
        #                     player_jsmatches_url=player_jsmatches_url,
        #                     retries=3,
        #                     delay=1
        #                 )
                    
        #             except Exception as e:
        #                 logging.info(f"Error when getting player jsmatches data for {player_jsmatches_url}: {e}.")
        #                 player_jsmatches_url_scrape_dict = {}

        #             # get data frm player jsmatches career scraping
        #             try:

        #                 player_jsmatches_career_url = create_player_jsmatches_career_url(
        #                     player_name=player_fullname
        #                 )
        #                 player_dict['player_jsmatches_career_url'] = player_jsmatches_career_url
        #                 logging.info(f"Getting player jsmatches career data from {player_jsmatches_career_url}.")
        #                 player_jsmatches_career_url_scrape_dict = get_player_jsmatches_career_data_scraped(
        #                     player_jsmatches_career_url=player_jsmatches_career_url,
        #                     retries=3,
        #                     delay=1
        #                 )
                    
        #             except Exception as e:
        #                 logging.info(f"Error when getting player jsmatches career data for {player_jsmatches_career_url}: {e}.")
        #                 player_jsmatches_career_url_scrape_dict = {}

        #             try:
                        
        #                 logging.info(f"Combining `matchmx` and `morematchmx` data.")
                        
        #                 # combine the 'matchmx' arrays if overlap
        #                 if player_jsmatches_url_scrape_dict['matchmx'] and player_jsmatches_career_url_scrape_dict['morematchmx']:
        #                     player_jsmatches_url_scrape_dict['matchmx'] = combine_list_of_dicts(
        #                         player_jsmatches_url_scrape_dict['matchmx'],
        #                         player_jsmatches_career_url_scrape_dict['morematchmx'],
        #                     )

        #             except Exception as e:
        #                 logging.info(f"Unable to combine `matchmx` and `morematchmx` data: {e}.")

        #     except Exception as e:
        #         logging.info(f"Unable to retrieve player jsmatches: {e}.")
        #         player_jsmatches_url_scrape_dict = {}

        #     # combine player scrape dicts into one
        #     # no need for 'jsmatches career' dict since it only contains morematchmx and that was combined with matchmx
        #     # ideally want player_classic_url_scrape_dict to override data
        #     player_scrape_dict = combine_dicts(
        #         player_jsmatches_url_scrape_dict,
        #         player_url_scrape_dict,
        #         player_classic_url_scrape_dict
        #     )

        #     # continue with player data logic if data is returned from scraping
        #     if player_scrape_dict != {}:

        #         logging.info(f"Data found for player: {player_name}.")

        #         # combine player data
        #         player_data_dict = combine_dicts(
        #             player_dict,
        #             player_scrape_dict,
        #         )

        #         # append to list
        #         player_data_list.append(player_data_dict)

        # return player_data_list

    except Exception as e:
        logging.error(f"Error when scraping player data: {e}.")
        return []