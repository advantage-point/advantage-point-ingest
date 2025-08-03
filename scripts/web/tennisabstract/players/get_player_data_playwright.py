from typing import (
    Dict
)
from playwright.sync_api import (
    Page,
)from utils.functions.web.scrape.scrape_javascript_var import scrape_javascript_var

import logging
import time

def get_player_data_playwright(
    player_url: str,
    page: Page,
    retries: int,
    delay: int
) -> Dict:
    """
    Arguments:
    - player_url: player link
    - page: Playwright page
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of player information from url
    """

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # initialize data to be retrieved
    response_var_list = ['nameparam', 'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast', 'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate', 'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link', 'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',]
    player_dict = {var: None for var in response_var_list}


    attempt = 0

    while attempt < retries:

        try:

            # navigate to the page
            # response = make_request(url=player_url)
            # response_text = response.text
            response = page.goto(
                player_url,
                wait_until='networkidle'
            )
            page_timeout = 5000 + (2500*attempt)
            page.wait_for_function("typeof nameparam !== 'undefined'", timeout=page_timeout)
            response_text = page.content()
            response_status_code = response.status if response else None
                
            for var in response_var_list:
                try:
                    val = scrape_javascript_var(
                        content=response_text,
                        var=var
                    )
                    player_dict[var] = val
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")

            # check if all values in dict are None
            if all(value is None for value in player_dict.values()):
                
                logging.info(f"All values None for {player_url}")
                
                # Log possible causes
                logging.debug(f"Page Content Length: {len(response_text)}")
                logging.debug(f"Response Status Code: {response_status_code}")
                
                # Log which variables were not found
                missing_vars = [var for var, val in player_dict.items() if val is None]
                logging.debug(f"Missing variables: {missing_vars}")

                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {player_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Max retries reached for {player_url}...Returning empty dictionary")
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
                logging.error(f"Max retries reached for {player_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}