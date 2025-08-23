from bs4 import BeautifulSoup
from typing import (
    Dict,
)
from utils.web.make_request import make_request
import logging
import re
import time

def get_tournament_data_scraped(
    tournament_url: str,
    retries: int = 3,
    delay: int = 3
) -> Dict:
    """
    Arguments:
    - tournament_url: Tournament link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of tournament information from url
    """

    # initialize data
    tournament_dict = {}

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=tournament_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # data is in the <p> tag with id 'biog'
            biog_element = soup.find(id = 'biog')
                
            # get the tournament title (<b>)
            try:
                tournament_title = biog_element.find('b').get_text(strip=True)
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable tournament_title: {e}")
                tournament_title = None
            tournament_dict["tournament_title"] = tournament_title

            # get the tournament start date: <month> <dd>, <yyyy>
            try:
                date_regex = re.compile(r'[A-Za-z]+\s+\d{1,2},\s+\d{4}')
                date_node = biog_element.find(string=date_regex)
                tournament_start_date = date_node.strip().split(' |')[0] # get rid of spaces and pipes
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable tournament_start_date: {e}")
                tournament_start_date = None
            tournament_dict["tournament_start_date"] = tournament_start_date

            # get the tournament surface; Surface: {surface}
            try:
                surface_regex = re.compile(r'Surface: ([A-Za-z]+)')
                surface_match = surface_regex.search(biog_element.get_text(separator=' ')) # use separator so that <br/> tags become spaces
                tournament_surface = surface_match.group(1).strip()
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable tournament_surface: {e}")
                tournament_surface = None
            tournament_dict["tournament_surface"] = tournament_surface

            # get the tournament draw size; Draw: {draw_size}
            try:
                draw_size_regex = re.compile(r'Draw: ([0-9]+)')
                draw_size_match = draw_size_regex.search(biog_element.get_text(separator=' ')) # use separator so that <br/> tags become spaces
                tournament_draw_size = draw_size_match.group(1).strip()
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable tournament_draw_size: {e}")
                tournament_draw_size = None
            tournament_dict["tournament_draw_size"] = tournament_draw_size

            # check if all values in dict are None
            if all(value is None for value in tournament_dict.values()):
                
                logging.info(f"All values None for {tournament_url}")
                
                # Log which variables were not found
                missing_vars = [var for var, val in tournament_dict.items() if val is None]
                logging.debug(f"Missing variables: {missing_vars}")

                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {tournament_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Max retries reached for {tournament_url}...Returning empty dictionary")
                return {}

            # return dictionary if data successfully extracted
            return tournament_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {tournament_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {tournament_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}