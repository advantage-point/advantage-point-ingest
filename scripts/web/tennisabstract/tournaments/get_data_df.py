from pandas import DataFrame
from scripts.web.tennisabstract.tournaments.get_tournament_data_scraped import get_tournament_data_scraped
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
    - url_list: List of tournament urls

    Create list of tournament data from list of tournament urls.
    """

    try:

        # loop through tournament urls
        tournament_data_list = []
        for i, tournament_dict in enumerate(url_list):

            tournament_url = tournament_dict['tournament_url']
            logging.info(f"({i+1}/{len(url_list)}) Getting tournament data for tournamnet url: {tournament_url}")

            # get data from tournament scraping
            tournament_scrape_dict = get_tournament_data_scraped(
                tournament_url=tournament_url,
                retries=3,
                delay=3
            )

            # continue with tournament data logic if data is returned from scraping
            if tournament_scrape_dict != {}:

                logging.info(f"Data found for tournament url: {tournament_url}")

                # combine tournament data
                tournament_data_dict = {
                    **tournament_dict,
                    **tournament_scrape_dict,
                }

                # append to list
                tournament_data_list.append(tournament_data_dict)

        # check if list is not empty
        if tournament_data_list != []:

            # create dataframe
            tournament_data_df = DataFrame.from_records(tournament_data_list)

            # stringify all values
            tournament_data_df = tournament_data_df.astype("string")

            return tournament_data_df

    except Exception as e:
        logging.error(f"Error when getting tournament data: {e}.")
        return DataFrame()