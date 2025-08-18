# from bs4 import BeautifulSoup
from typing import (
    Dict,
)
from utils.web.make_request import make_request
from utils.web.scrape_javascript_var import (
    scrape_javascript_var,
)
import logging
import time

def get_player_data_scraped(
    player_url: str,
    retries: int = 3,
    delay: int = 3
) -> Dict:
    """
    Arguments:
    - player_url: player link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of player information from url
    """

    # initialize data
    player_dict = {}

    attempt = 0

    while attempt < retries:

        try:

            response_var_list = [
                'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast',
                'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate',
                'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link',
                'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',
            ]
            player_dict = {var: None for var in response_var_list}

            # navigate to the page
            response = make_request(url=player_url)
            response_text = response.text
            # soup = BeautifulSoup(response_text, 'html.parser')

            for var in response_var_list:
                try:
                    val = scrape_javascript_var(
                        content=response_text,
                        var=var
                    )

                    # # add extra quotes around the value if not exists
                    # if (val is not None) and ("'" not in val) and ('"' not in val) :
                    #     val = f"'{val}'"

                    player_dict[var] = val
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")
                    continue

            # check if all values in dict are None
            if all(value is None for value in player_dict.values()):
                
                logging.info(f"All values None for: {player_url}")
                
                # Log which variables were not found
                missing_vars = [var for var, val in player_dict.items() if val is None]
                logging.info(f"Missing variables: {missing_vars}")

                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {player_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.info(f"Max retries reached for {player_url}...Returning empty dictionary")
                return {}

            # return dictionary if data successfully extracted
            return player_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {player_url}: {e}")

            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.info(f"Max retries reached for {player_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}