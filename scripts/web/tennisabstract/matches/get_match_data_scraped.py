from bs4 import BeautifulSoup
from scripts.web.tennisabstract.matches.parse_match_pointlog import parse_match_pointlog
from typing import (
    Dict,
)
from utils.web.make_request import make_request
from utils.web.scrape_javascript_var import scrape_javascript_var
import logging
import re
import time

def get_match_data_scraped(
    match_url: str,
    retries: int = 3,
    delay: int = 3
) -> Dict:
    """
    Arguments:
    - match_url: match link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of match information from url
    """

    # initialize data
    match_dict = {}

    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            response = make_request(url=match_url)
            soup = BeautifulSoup(response.text, 'html.parser')
                
            # get the match title (<title>): <match info>: <player1> vs <player2> Detailed Stats | Tennis Abstract
            try:
                match_title = soup.find('title').text.split(' Detailed Stats | Tennis Abstract')[0]
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable `match_title`: {e}.")
                match_title = None
            match_dict["match_title"] = match_title

            # get the match result (b): <winner> d. <loser> score
            try:
                match_result = soup.find('b', string=re.compile(r".+\sd\.\s.+\s.+")).text
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable `match_result`: {e}.")
                match_result = None
            match_dict["match_result"] = match_result

            # get pointlog
            try:
                match_pointlog_html = scrape_javascript_var(
                    content=response.text,
                    var='pointlog'
                )
                match_pointlog = parse_match_pointlog(pointlog_html=match_pointlog_html)
            except Exception as e:
                logging.info(f"Error encountered when getting data for variable `match_pointlog`: {e}.")
                match_pointlog = []
            match_dict['match_pointlog'] = match_pointlog            

            # check if all values in dict are None -> return empty dict
            if all(value is None for value in match_dict.values()):
                logging.info(f"All values None for {match_url} - Returning empty dictionary.")
                return {}

            # return dictionary if data successfully extracted
            return match_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {match_url}: {e}")
            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.error(f"Max retries reached for {match_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}