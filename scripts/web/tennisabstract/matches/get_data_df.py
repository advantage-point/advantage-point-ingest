from pandas import DataFrame
from scripts.web.tennisabstract.matches.get_match_data_scraped import get_match_data_scraped
from typing import (
    Dict,
    List,
)
import logging

def main(
    url_list: List[Dict]
) -> DataFrame:
    """
    Arguments:
    - url_list: List of match urls

    Create list of match data from list of match urls.
    """

    try:

        # loop through match urls
        match_data_list = []
        for i, match_dict in enumerate(url_list):

            match_url = match_dict['match_url']
            logging.info(f"({i+1}/{len(url_list)}) Getting match data for match url: {match_url}")

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
                match_data_dict = {
                    **match_dict,
                    **match_scrape_dict,
                }

                # append to list
                match_data_list.append(match_data_dict)

        # load data to database
        if match_data_list != []:

            # create dataframe
            match_data_df = DataFrame.from_records(match_data_list)

            # replace nulls with empty strings
            match_data_df = match_data_df.fillna("")

            # cast columns to string
            match_data_df = match_data_df.astype("string")

            return match_data_df

    except Exception as e:
        logging.error(f"Error when getting match data: {e}.")
        return DataFrame()