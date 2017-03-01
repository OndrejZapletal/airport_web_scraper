#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

from threading import Thread, RLock
from queue import Queue
import csv
import sys
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
    try:
        req = Request(compose_request(airport_key), headers={'User-Agent' : "kiwi project"})
        response = urlopen(req)
        country_code = parse_country_code(response.read())
        if country_code:
            with lock:
                dictionary_of_airports[airport_key] = country_code
        else:
            print("Counld not find country for '%s' airport" % airport_key)
    except URLError as error:
        print("Error: %s" % error)



def parse_country_code(response):
    """returns code of country"""
    soup = BeautifulSoup(response, "html.parser")
    result = str(soup.find_all("div", "header clearfix")[0])
    soup = BeautifulSoup(result, "html.parser")
    result = str(soup.find("p").contents[0][1:-1])
    try:
        code = COUNTRY_RE.match(result).group(1)
    except AttributeError:
        print("result: %s" % result)
        code = None
    return code


def get_dictionary_of_airports(list_of_airports, multithreded):
    """Creates dictionary containg 'airport code' : 'country' pairs."""
    if multithreded:
        print("Starting multithreaded version")
        threads = []

        for airport in list_of_airports:
            threads.append(Thread(target=get_airport_country, args=(airport,)))

        for thread in threads:
            thread.daemon = True
            thread.start()

        for thread in threads:
            thread.join()
    else:
        print("Starting single thread version")
        for airport in list_of_airports:
            get_airport_country(airport)



def get_list_of_airports():
    """get list of airports from input data file."""
    list_of_airports = []
    with open("input_data.csv", 'r') as csvfile:
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
    multithreaded = True
    if len(sys.argv) > 1:
        if sys.argv[1] == "1":
            multithreaded = False

    airports = get_list_of_airports()
    print("Gathering information about %s airports" % len(airports))
    get_dictionary_of_airports(airports, multithreaded)

    airport_list_text = ""
    for airport, country in dictionary_of_airports.items():
        airport_list_text += "%s:%s\n" % (airport, country)

    with open("airport_names.txt", "w") as airports_file:
        airports_file.write(airport_list_text)


lock = RLock()
dictionary_of_airports = {}
# if __name__ == "__main__":
#     main()

main()
