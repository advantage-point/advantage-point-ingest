from typing import (
    Dict,
    List,
)
from scripts.web.tennisabstract.matches.get_match_data_scraped import get_match_data_scraped
from utils.python.combine_dicts import combine_dicts
import logging

def get_match_data(
    match_url_list: List[Dict]
) -> List[Dict]:
    """
    Arguments:
    - match_url_list: List of match urls

    Create list of match data from list of match urls.
    """

    try:

        # loop through match urls
        match_data_list = []
        for i, match_dict in enumerate(match_url_list):

            match_url = match_dict['match_url']
            logging.info(f"({i+1}/{len(match_url_list)}) Getting match data for match url: {match_url}")

            # # get data from match url
            # match_url_dict = get_match_data_url(match_url=match_url)
            # logging.info(f"Got match url data for match url: {match_url}")

            # get data from match scraping
            match_scrape_dict = get_match_data_scraped(
                match_url=match_url,
                retries=3,
                delay=3
            )

            # continue with match data logic if data is returned from scraping
            if match_scrape_dict != {}:

                logging.info(f"Data found for match url: {match_url}")

                # combine match data
                match_data_dict = combine_dicts(
                    match_dict,
                    match_scrape_dict
                )

                # append to list
                match_data_list.append(match_data_dict)

        return match_data_list

    except Exception as e:
        logging.error(f"Error when scraping match data: {e}.")
        return []