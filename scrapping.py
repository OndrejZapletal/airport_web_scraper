"""Web scrapping part of the application"""
import csv
import operator
import os
import re
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from time import sleep
from urllib.request import Request, URLError, urlopen

from bs4 import BeautifulSoup
from pytz import country_timezones, timezone, utc
from pytz.exceptions import AmbiguousTimeError

ADDRESS = "https://www.world-airport-codes.com/"
AIRPORT_LIST_FILE = "airport_list.txt"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")
CSV_INPUT_FILE = "input_data.csv"
DT_INPUT_FORMAT = "%Y-%m-%d %H:%M:%S"
NUMBER_OF_RETRIES = 5
SIZE_OF_POOL = 50

FlightTuple = namedtuple("FlightTuple", [
    'from_airport', 'from_country', 'from_date', 'to_airport', 'to_country', 'to_date'])


def get_list_of_airports():
    """get list of airports from input data file."""
    list_of_airports = []
    with open(CSV_INPUT_FILE, 'r') as csv_file:
        flights_reader = csv.reader(csv_file, delimiter=";")
        next(flights_reader, None)  # skip header
        for row in flights_reader:
            if row[0] not in list_of_airports:  # origin airports
                list_of_airports.append(row[0])

            if row[1] not in list_of_airports:  # destination airports
                list_of_airports.append(row[1])
    return list_of_airports


def compose_request(airport):
    """compose string for search request"""
    if len(airport) != 3:
        raise Exception("Airport key '%s' is not valid" % airport)
    return "{}search/?s={}".format(ADDRESS, airport.lower())


def parse_country_code(response):
    """returns code of country"""
    try:
        soup = BeautifulSoup(response, "html.parser")
        result = str(soup.find_all("div", "header clearfix")[0])
        soup = BeautifulSoup(result, "html.parser")
        result = str(soup.find("p").contents[0][1:-1])
        try:
            code = COUNTRY_RE.match(result).group(1)
        except AttributeError:
            code = None

    except UnboundLocalError:
        code = None
    except IndexError:
        code = None
    return code


def send_request(req, airport):
    """Function tries to send request 3 times before it moves on"""
    tries = 0
    response = ""
    while tries < NUMBER_OF_RETRIES:
        try:
            response = urlopen(req).read()
            break
        except URLError as error:
            print("Error during %s try of sending request for airport '%s': %s"
                  % (tries, airport, error))
            tries += 1
            response = ""
            sleep(2)
    return response


def get_airport_country(airport):
    """Fetch airport info from www.world-airport-codes.com."""
    req = Request(
        compose_request(airport), headers={'User-Agent': "kiwi project"})
    response = send_request(req, airport)
    country_code = parse_country_code(response)
    if country_code:
        return airport, country_code
    else:
        return None


def get_dictionary_of_airports(list_of_airports):
    """Creates dictionary containing 'airport code' : 'country' pairs.

    Because the scrapping from web takes a long time. The result is saved
    to file and read on the next try.
    """
    dictionary_of_airports = {}
    # if there is no file with Airport countries
    if not os.path.isfile(AIRPORT_LIST_FILE):
        with ThreadPoolExecutor(max_workers=SIZE_OF_POOL) as thread_pool:
            results = list(
                thread_pool.map(get_airport_country, list_of_airports))
            airport_list_text = ""
            for result in results:
                if result:
                    airport_list_text += "%s:%s\n" % (result[0], result[1])
                    dictionary_of_airports[result[0]] = result[1]

            with open(AIRPORT_LIST_FILE, "w") as airports_file:
                airports_file.write(airport_list_text)
    # when file with Airport countries already exists
    else:
        with open(AIRPORT_LIST_FILE, "r") as airports_file:
            for line in airports_file:
                airport, country = line[:-1].split(":")
                dictionary_of_airports[airport] = country
    return dictionary_of_airports


def get_list_of_flights(dictionary_of_airports):
    """creates list of flights"""
    list_of_flights = []
    with open(CSV_INPUT_FILE, 'r') as csv_file:
        flights_reader = csv.reader(csv_file, delimiter=";")
        next(flights_reader, None)  # skip header
        for row in flights_reader:
            flight_data = construct_flight_data(row, dictionary_of_airports)
            if flight_data:
                list_of_flights.append(flight_data)

    return list_of_flights


def set_local_date_time(datetime_input, country):
    """Convert UTC time into local time based on country. """
    # creates country time zone object
    local = timezone(country_timezones(country)[0])
    # creates naive datetime object
    naive = datetime.strptime(datetime_input, DT_INPUT_FORMAT)
    # localize naive datetime object
    local_dt = local.localize(naive, is_dst=None)
    # return UTC datetime object
    return local_dt.astimezone(utc)


def construct_flight_data(values, dictionary_of_airports):
    """Fill namedtuple from line of CSV files."""
    try:
        data = FlightTuple(
            from_airport=values[0],
            from_country=dictionary_of_airports[values[0]],
            from_date=set_local_date_time(values[2],
                                          dictionary_of_airports[values[0]]),
            to_airport=values[1],
            to_country=dictionary_of_airports[values[1]],
            to_date=set_local_date_time(values[3],
                                        dictionary_of_airports[values[1]]))
    except KeyError:
        # Time zone information about one of the countries is missing from database.
        return None
    except AmbiguousTimeError:
        # Some none obvious problem with datatime conversion.
        return None
    return data


def select_appropriate_flights(flights):
    """Flights that start and end in the same country are no valid."""
    valid_flights = [
        flight for flight in flights
        if flight.from_country != flight.to_country
    ]
    return valid_flights


def get_flight_information():
    """gathers information about flights"""
    airports = get_list_of_airports()
    # creates dictionary with countires of individual airports
    airport_countries = get_dictionary_of_airports(airports)
    # creates list of flights
    list_of_flights = get_list_of_flights(airport_countries)
    # selects only those flights that fly into different country
    valid_flights = select_appropriate_flights(list_of_flights)

    return sorted(valid_flights, key=operator.attrgetter('from_date'))
