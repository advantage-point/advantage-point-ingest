from pandas import DataFrame
from scripts.web.tennisabstract.players.get_player_data_scraped import (
    get_player_data_scraped,
)
from typing import (
    Dict,
    List,
)
import logging

def get_player_data(
    player_url_list: List[Dict]
) -> DataFrame:
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

            player_name = player_dict['player_name']
            logging.info(f"({i+1}/{len(player_url_list)}) Getting player data for player: {player_name}.")

            # get data from player scraping
            try:
                player_url = player_dict['player_url']
                logging.info(f"Getting player data from {player_url}.")
                player_url_scrape_dict = get_player_data_scraped(
                    player_url=player_url,
                    retries=3,
                    delay=1
                )
            
            except Exception as e:
                logging.info(f"Error getting player data from {player_url}: {e}.")
                player_url_scrape_dict = {}

            # continue with player data logic if data is returned from scraping
            if player_url_scrape_dict != {}:

                logging.info(f"Data found for player: {player_name}.")

                # combine player data
                player_data_dict = {
                    **player_dict,
                    **player_url_scrape_dict,
                }

                # append to list
                player_data_list.append(player_data_dict)

        # check if list is not empty
        if player_data_list != []:

            # create dataframe
            player_data_df = DataFrame.from_records(player_data_list)

            # stringify all values
            player_data_df = player_data_df.astype("string")

            return player_data_df

    except Exception as e:
        logging.error(f"Error when getting player data: {e}.")
        return DataFrame()