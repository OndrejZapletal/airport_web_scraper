#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

from threading import Thread, Lock, RLock
import csv
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
    return request


def get_airport_country(airport_key):
    """Fetch airport info from www.world-airport-codes.com."""
    print("get airport country for: %s", airport_key)
    req = Request(compose_request(airport_key), headers={'User-Agent' : "kiwi project"})
    response = urlopen(req)
    country_code = parse_country_code(response.read())
    with lock:
        dictionary_of_airports[airport_key] = country_code
    print(country_code)


def parse_country_code(response):
    """returns code of country"""
    soup = BeautifulSoup(response, "html.parser")
    result = str(soup.find_all("div", "header clearfix")[0])
    soup = BeautifulSoup(result, "html.parser")
    result = soup.find("p").contents[0][1:-1]
    return str(COUNTRY_RE.match(result).group(1))


def get_dictionary_of_airports(list_of_airports):
    """Creates dictionary containg 'airport code' : 'country' pairs."""
    threads = []

    for airport in list_of_airports:
        threads.append(Thread(target=get_airport_country, args=(airport,)))

    for thread in threads:
        thread.daemon = True
        thread.start()

    for thread in threads:
        thread.join()


def get_list_of_airports():
    """get list of airports from input data file."""
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
    return list_of_airports


def main():
    """main function"""
    airports = get_list_of_airports()
    get_dictionary_of_airports(airports)
    airport_countries = dictionary_of_airports
    print(airport_countries)

    airport_list_text = ""
    for airport, country in airport_countries.items():
        airport_list_text += "%s:%s\n" % (airport, country)

    with open("airport_names.txt", "w") as airports_file:
        airports_file.write(airport_list_text)


lock = RLock()
dictionary_of_airports = {}
# if __name__ == "__main__":
#     main()

main()
