#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

import csv
import re

from urllib.request import Request, urlopen, URLError
from bs4 import BeautifulSoup

import ipdb

ADDRESS = "https://www.world-airport-codes.com/"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")


def compose_request(airport_key):
    """compose string for search request"""
    if len(airport_key) != 3:
        raise Exception("Airport key '%s' is not valid" % airport_key)
    request = "%ssearch/?s=%s" % (ADDRESS, airport_key.lower())
    return request


def get_airport_country(airport_key):
    """Fetch airport info from www.world-airport-codes.com."""
    req = Request(compose_request(airport_key), headers={'User-Agent' : "kiwi project"})
    response = urlopen(req)
    return parse_country_code(response.read())


def parse_country_code(response):
    """returns code of country"""
    soup = BeautifulSoup(response, "html.parser")
    result = str(soup.find_all("div", "header clearfix")[0])
    soup = BeautifulSoup(result, "html.parser")
    result = soup.find("p").contents[0][1:-1]
    return str(COUNTRY_RE.match(result).group(1))


def get_dictionary_of_airports(list_of_airports):
    """Creates dictionary containg 'airport code' : 'country' pairs."""
    airport_countries = {}
    for airport in list_of_airports:
        try:
            airport_countries[airport] = get_airport_country(airport)
        except URLError as error:
            print("Request error: %s" % error)

    return airport_countries



def main():
    """main function"""
    list_of_airports = []

    with open("input_data_short.csv", 'r') as csvfile:
        flights_reader = csv.reader(csvfile, delimiter=";")
        # ipdb.set_trace(context=10)
        next(flights_reader, None) # skip header
        for row in flights_reader:
            if row[0] not in list_of_airports:
                list_of_airports.append(row[0])

            if row[1] not in list_of_airports:
                list_of_airports.append(row[1])

    airport_list_text = ""

    for airport, country in get_dictionary_of_airports(list_of_airports).items():
        airport_list_text += "%s:%s\n" % (airport, country)

    with open("airport_names.txt", "w") as airports_file:
        airports_file.write(airport_list_text)

    # print("number of airports: %s" % len(list_of_airports))


# if __name__ == "__main__":
#     main()

main()
