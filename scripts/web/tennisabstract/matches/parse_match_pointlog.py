from bs4 import BeautifulSoup
from typing import (
    Dict,
    List,
)
import logging

def parse_match_pointlog(
        pointlog_html: str
) -> List[Dict]:
    """
    Arguments:
    - pointlog_html: HTML string containing pointlog data

    Parses HTML and returns list of point dictionaries.
    """

    try:

        point_data_list = []
        
        # extract the data (after 1st tr - headers)
        pointlog_soup = BeautifulSoup(pointlog_html, 'html.parser')
        pointlog_tr_list = pointlog_soup.find_all('tr')[1:]

        # filter out empty rows
        pointlog_tr_list = [
            tr for tr in pointlog_tr_list 
            if all(td.get_text(strip=True) for td in tr.find_all('td'))
        ]

        # loop through tr list
        for index, tr in enumerate(pointlog_tr_list):
            tr_td_list = tr.find_all('td')
            point_data = {
                'point_number': index + 1,
                'server': tr_td_list[0].get_text(strip=True),
                'sets': tr_td_list[1].get_text(strip=True),
                'games': tr_td_list[2].get_text(strip=True),
                'points': tr_td_list[3].get_text(strip=True),
                'point_description': tr_td_list[4].get_text(strip=True),
            }
            point_data_list.append(point_data)
        return point_data_list
    except Exception as e:
        logging.info(f"Error getting point data.: {e}")
        return []