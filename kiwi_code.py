#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

import csv
# import ipdb
import re
from urllib.request import Request, urlopen, URLError
from bs4 import BeautifulSoup

ADDRESS = "https://www.world-airport-codes.com/"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")

def compose_request(airport_key):
    """compose string for search request"""
    if len(airport_key) != 3:
        raise Exception("Airport key '%s' is not valid" % airport_key)
    request = "%ssearch/?s=%s" % (ADDRESS, airport_key.lower())
    print(request)
    return request


def get_airport_info(airport_key):
    """function tries to get API response"""
    response_text = ""
    try:
        req = Request(compose_request(airport_key), headers={'User-Agent' : "kiwi project"})
        response = urlopen(req)
        response_text = get_country_code(response.read())
    except URLError as error:
        response_text = airport_key
        print("Request error: %s" % error)
    return response_text

def get_country_code(response):
    soup = BeautifulSoup(response, "html")
    result = soup.find_all("div", "header clearfix")
    soup = BeautifulSoup(str(result[0]), "html")
    result = soup.find("p").contents[0][1:-1]

    return str(COUNTRY_RE.match(result).group(1))


def main():
    """main function"""
    list_of_airports = []

    with open("input_data_short.csv", 'r') as csvfile:
        flights_reader = csv.reader(csvfile, delimiter=";")
        for row in flights_reader:
            if row[0] not in list_of_airports:
                list_of_airports.append(row[0])

            if row[1] not in list_of_airports:
                list_of_airports.append(row[1])

    print("number of airports: %s" % len(list_of_airports))

    response_text = ""

    with open("airport_names.json", "w") as airports_file:
        for item in list_of_airports[2:]:
            response_text = get_airport_info(item)
            print("'%s'" % response_text)
            airports_file.write("%s\n" % response_text)
            # print(response_text)


# https://iatacodes.org/api/v6/airports?api_key=2b58201b-0a1b-4ca6-8150-67e789949d0f&code=brn
# https://iatacodes.org/api/v6/countries?api_key=2b58201b-0a1b-4ca6-8150-67e789949d0f&code=brn

    # for item in list_of_airports:
    #     print(item)

# if __name__ == "__main__":
#     main()

main()
