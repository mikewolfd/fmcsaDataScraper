
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re


def to_snakecase(string, regex):
    return '_'.join(regex.sub('_', string).split('_')).strip('_').lower()


def get_cargo(soup):
    """ 
    This searchs the html page for an html list with the class 'cargo'.
    The active li item has a class 'checked'.
    This strips non-alphanumeric values, converts strings to snakecase, and numerical values to ints.

    Sample Output:

    {   'general_freight': True,
        'household_goods': False,
        'metal_sheets_coils_rolls': False,
        'motor_vehicles': False, ...
        }

    """
    data = {}
    im = re.compile(r'\W+')
    cargo = soup.find("ul", attrs={"class": "cargo"}).find_all('li')
    for i in cargo:
        active = "checked" in i.get('class')
        val = i.text
        if active:
            val = val.lstrip('X').strip()
        data[to_snakecase(val, im)] = active
    return data


def get_vehicle_type(soup):
    """ 
    This searchs the html page for the only table, which happens to be the vehicle type.
    Pandas has a from_html feature that does something similar, but this gave me more granularity.
    This strips non-alphanumeric values, converts strings to snakecase, and numerical values to ints.

    Sample Output:

    [{'vehicle_type': 'straight_trucks',
    'owned': 98785,
    'term_leased': 0,
    'trip_leased': 0}, ...]

    """
    data = []
    # precompiling a regex is faster, this looks for all non-alphanumeric values
    im = re.compile(r'\W+')
    cargo = soup.find('table')
    headers = []
    for th in cargo.thead.find_all('th'):
        headers.append(to_snakecase(th.text, im))
    for row in cargo.tbody.find_all('tr'):
        row_data = {}
        row_items = row.text.strip().split('\n')
        for item, header in zip(row_items, headers):
            pos_int = to_snakecase(item, im)
            try:
                # int accepts and strips underscores which would've been created from the snakecase
                pos_int = int(pos_int)
            except ValueError:
                pass
            row_data[header] = pos_int
        data.append(row_data)
    return data


def get_carrier_registration(carrier_id):
    page = requests.get(
        'https://ai.fmcsa.dot.gov/SMS/Carrier/{}/CarrierRegistration.aspx'.format(carrier_id)).text
    soup = BeautifulSoup(page)
    return {'carrier_id': carrier_id, 'cargo': get_cargo(soup), 'types': get_vehicle_type(soup)}
