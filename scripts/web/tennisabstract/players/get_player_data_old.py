from scripts.web.tennisabstract.players.get_player_data_playwright import get_player_data_playwright
from typing import (
    Dict,
    List,
)
from utils.web.create_playwright_page import create_playwright_page
import logging

def get_player_data(
    player_url_list: List[Dict]
) -> List[Dict]:
    """
    Arguments:
    - player_url_list: List of player urls

    Create pandas dataframe from list.
    """

    try:

        # create playwright page
        playwright, browser, page = create_playwright_page()

        # loop through player url list
        player_data_list = []
        for i, player_url_dict in enumerate(player_url_list):

            logging.info(f"({i+1}/{len(player_url_list)}) Getting player data")

            player_url = player_url_dict['player_url']
            logging.info(f"Getting player data for player url: {player_url}")

            # get data from player scraping
            player_scrape_dict = get_player_data_playwright(
                player_url=player_url,
                page=page,
                retries=3,
                delay=3,
            )

            # continue with player data logic if data is returned from scraping
            if player_scrape_dict != {}:

                logging.info(f"Data found for player url: {player_url}")

                # combine player data
                player_data_dict = {
                    **player_url_dict,
                    **player_scrape_dict,
                }

                # append to player list
                player_data_list.append(player_data_dict)

        return player_data_list

    except Exception as e:
        logging.error(f"Error when scraping match data: {e}.")
        return []
    
    finally:
        
        # stop playwright
        browser.close()
        playwright.stop()