# from bs4 import BeautifulSoup
from typing import (
    Dict,
)
from scripts.web.tennisabstract.players.parse_player_matchmx import (
    parse_player_classic_matchmx,
)
from utils.web.make_request import make_request
from utils.web.scrape_javascript_var import (
    scrape_javascript_multiline_var,
    scrape_javascript_var,
)
import ast
import json
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

                    # add extra quotes around the value if not exists
                    if (val is not None) and ("'" not in val) and ('"' not in val) :
                        val = f"'{val}'"

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

def get_player_classic_data_scraped(
    player_classic_url: str,
    retries: int = 3,
    delay: int = 3
) -> Dict:
    """
    Arguments:
    - player_classic_url: player classic link
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
                'matchmx',
            ]
            player_dict = {var: None for var in response_var_list}

            # navigate to the page
            response = make_request(url=player_classic_url)
            response_text = response.text
            # soup = BeautifulSoup(response_text, 'html.parser')

            for var in response_var_list:
                try:
                    # parse matchmx
                    if var == 'matchmx':
                        try:
                            val = scrape_javascript_multiline_var(
                                content=response_text,
                                var=var
                            )

                            # convert to string if not null
                            if val is not None:
                                val = ast.literal_eval(val)
                                val = parse_player_classic_matchmx(player_matchmx_list=val)
                                val = json.dumps(val)
                        except Exception as e:
                            logging.error(f"Error scraping {var}: {e}.")
                            

                    else:
                        val = scrape_javascript_var(
                            content=response_text,
                            var=var
                        )

                        # convert to string: add extra quotes around the value if not exists
                        if (val is not None) and ("'" not in val) and ('"' not in val) :
                            val = f"'{val}'"
                        
                    player_dict[var] = val
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")
                    continue

            # check if all values in dict are None
            if all(value is None for value in player_dict.values()):
                
                logging.info(f"All values None for: {player_classic_url}")
                
                # Log which variables were not found
                missing_vars = [var for var, val in player_dict.items() if val is None]
                logging.info(f"Missing variables: {missing_vars}")

                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {player_classic_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.info(f"Max retries reached for {player_classic_url}...Returning empty dictionary")
                return {}

            # return dictionary if data successfully extracted
            return player_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {player_classic_url}: {e}")

            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.info(f"Max retries reached for {player_classic_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}

def get_player_data_scraped_test(
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

    attempt = 0

    while attempt < retries:

        try:

            response_var_list = [
                'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast',
                'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate',
                'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link',
                'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',
            ]

            # initialize data
            player_dict = {}

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

                    # check if value exists
                    if (val is not None) and (val != '') and (val != "") and (val != "''") and (val != '""'):
                        # check if quotes exist in string
                        if ('"' not in val) and ("'" not in val):
                            val = f'"{val}"'
                        
                        # add to dict
                        player_dict[var] = val
                
                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")
                    continue

            # check if dict is empty
            if player_dict == {}:
                
                logging.info(f"All values null for: {player_url}")

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

def get_player_classic_data_scraped_test(
    player_classic_url: str,
    retries: int = 3,
    delay: int = 3
) -> Dict:
    """
    Arguments:
    - player_classic_url: player classic link
    - retries: Number of retry attempts
    - delay: Time (in seconds) between retries

    Returns dictionary of player information from url
    """

    attempt = 0

    while attempt < retries:

        try:

            response_var_list = [
                'fullname', 'lastname', 'currentrank', 'peakrank', 'peakfirst', 'peaklast',
                'dob', 'ht', 'hand', 'backhand', 'country', 'shortlist', 'careerjs', 'active', 'lastdate',
                'twitter', 'current_dubs', 'peak_dubs', 'peakfirst_dubs', 'liverank', 'chartagg', 'photog', 'photog_credit', 'photog_link',
                'itf_id', 'atp_id', 'dc_id', 'wiki_id', 'elo_rating', 'elo_rank',
                'matchmx',
            ]
            
            # initialize data
            player_dict = {}

            # navigate to the page
            response = make_request(url=player_classic_url)
            response_text = response.text
            # soup = BeautifulSoup(response_text, 'html.parser')

            for var in response_var_list:
                try:
                    # parse matchmx
                    if var == 'matchmx':
                        try:
                            val = scrape_javascript_multiline_var(
                                content=response_text,
                                var=var
                            )

                            # convert to string if not null
                            if val is not None:
                                val = ast.literal_eval(val)
                                val = parse_player_classic_matchmx(player_matchmx_list=val)
                                val = json.dumps(val)
                                # append to dict
                                player_dict[var] = val

                        except Exception as e:
                            logging.error(f"Error scraping {var}: {e}.")
                            

                    else:
                        val = scrape_javascript_var(
                            content=response_text,
                            var=var
                        )

                        # check if value exists
                        if (val is not None) and (val != '') and (val != "") and (val != "''") and (val != '""'):
                            # check if quotes exist in string
                            if ('"' not in val) and ("'" not in val):
                                val = f'"{val}"'
                        
                            # append to dict
                            player_dict[var] = val

                except Exception as e:
                    logging.info(f"Error encountered when getting data for variable {var}: {e}")
                    continue

            # check if dict is empty
            if player_dict == {}:
                
                logging.info(f"All values null for: {player_classic_url}")
                
                attempt += 1
                
                # retry if possible
                if attempt < retries:
                    logging.info(f"Retrying in {delay} seconds...Attempt {attempt} for {player_classic_url}")
                    time.sleep(delay)
                    continue
                else:
                    logging.info(f"Max retries reached for {player_classic_url}...Returning empty dictionary")
                return {}

            # return dictionary if data successfully extracted
            return player_dict

        except Exception as e:
            attempt += 1
            logging.warning(f"Attempt {attempt} failed for {player_classic_url}: {e}")

            if attempt < retries:
                logging.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)  # Delay before retrying
            else:
                logging.info(f"Max retries reached for {player_classic_url}.")

    # Return empty dictionary if all retries fail
    logging.info(f"Returning empty dictionary")
    return {}