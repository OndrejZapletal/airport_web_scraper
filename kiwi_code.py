#!/usr/bin/env python3

"""Solution of kiwi code challenge."""

import csv
import operator
import os
# import random
import re
from collections import namedtuple
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from queue import Queue
from threading import RLock, Thread
from time import sleep
from urllib.request import Request, URLError, urlopen

from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta

ADDRESS = "https://www.world-airport-codes.com/"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")
DT_INPUT_FORMAT = "%Y-%m-%d %H:%M:%S"
DT_OUTPUT_FORMAT = "%Y-%m-%dT%H:%M"
CSV_INPUT = "input_data.csv"
NUMBER_OF_CANDIDATES_PER_SEARCH = 4
NUMBER_OF_RETRIES = 5
NUMBER_OF_JOURNEY_STARTS = 1000
LENGTH_OF_COMPLETE_JOURNEY = 10

FlightTuple = namedtuple("FlightTuple",
                         ['from_airport',
                          'from_country',
                          'from_date',
                          'to_airport',
                          'to_country',
                          'to_date'])

class Worker(Thread):
    """ Thread executing tasks from a given tasks queue """
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()

    def run(self):
        while True:
            func, args, kargs = self.tasks.get()
            try:
                func(*args, **kargs)
            except Exception as error:
                # An exception happened in this thread
                print("Error during thread execution for airport '%s': %s" % (args, error))
            finally:
                # Mark this task as done, whether an exception happened or not
                self.tasks.task_done()


class ThreadPool(object):
    """ Pool of threads consuming tasks from a queue """
    def __init__(self, num_threads):
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            Worker(self.tasks)

    def add_task(self, func, *args, **kargs):
        """ Add a task to the queue """
        self.tasks.put((func, args, kargs))

    def map(self, func, args_list):
        """ Add a list of tasks to the queue """
        for args in args_list:
            self.add_task(func, args)

    def wait_completion(self):
        """ Wait for completion of all the tasks in the queue """
        self.tasks.join()


def construct_flight_data(values):
    """Fill namedtuple from line of CSV files."""
    return FlightTuple(from_airport=values[0],
                       from_country=dictionary_of_airports[values[0]],
                       from_date=datetime.strptime(values[2], DT_INPUT_FORMAT),
                       to_airport=values[1],
                       to_country=dictionary_of_airports[values[1]],
                       to_date=datetime.strptime(values[3], DT_INPUT_FORMAT))


def filter_younger_then_year(journey, list_of_flights):
    """Function returns only flights that arrive before 1 year +- 24 hours
    from start of the journey.
    """
    return [candidate for candidate in list_of_flights
            if relativedelta(candidate.to_date - timedelta(days=1),
                             journey[0].from_date).years < 1]


def return_flight_data(flight_data):
    """format flight data into readable format."""
    return "{};{};{};{};{}".format(
        flight_data.from_country,
        flight_data.from_airport,
        flight_data.to_airport,
        datetime.strftime(flight_data.from_date, DT_OUTPUT_FORMAT),
        datetime.strftime(flight_data.to_date, DT_OUTPUT_FORMAT))


def return_in_local_format(flight_data):
    """format flight data into readable format."""
    return "{};{};{};{};{}".format(
        flight_data.from_country,
        flight_data.from_airport,
        flight_data.to_airport,
        get_local_date_time(flight_data.from_date, flight_data.from_country),
        get_local_date_time(flight_data.to_date, flight_data.to_country))


def get_local_date_time(datetime_input, country):
    """Convert UTC time into local time based on country. """
    return datetime.strftime(datetime_input, DT_OUTPUT_FORMAT)


def write_journeys_to_file(list_of_journeys):
    """Write found journeys into file."""
    with open("journeys.txt", "w") as file:
        for index, journey in enumerate(list_of_journeys):
            for flight in journey:
                file.write("%s;%s\n" % (index+1, return_flight_data(flight)))


def analyze_routes(flights):
    """analyze possible routes"""
    possible_journeys = []
    # select only smaller number of starting flights
    # for index, flight in enumerate(random.sample(flights, NUMBER_OF_JOURNEY_STARTS)):
    #     print("%s flight of %s" % (index+1, NUMBER_OF_JOURNEY_STARTS))
    #     journey = list([flight])
    #     possible_journeys += find_route((journey, flights))

    # Creation of these lists is little bit convoluted by it is necessary for
    # serialization for ProcessPoolExecutor
    flights = down_sample(flights, NUMBER_OF_JOURNEY_STARTS)
    journeys = [list([flight]) for flight in flights]
    flights_list = [flights for i in range(len(flights))]
    args = list(zip(journeys, flights_list))

    with  ProcessPoolExecutor() as executor:
        possible_journeys = list(executor.map(find_route, args))

    return possible_journeys


def find_route(args):
    """Recursive function investigates potential journey."""
    if len(args[0]) == LENGTH_OF_COMPLETE_JOURNEY - 1:
        return validate_journey(args[0], args[1])
    else:
        journeys = []
        for candidate in filter_candidates(args[0], args[1]):
            journeys += find_route((extend(args[0], candidate), args[1]))
        return journeys


def validate_journey(journey, list_of_flights):
    """List of all possible flights is filtered to only valid candidates."""
    candidates = [candidate for candidate in list_of_flights
                  # Select flights departing after arrival of last flight in the journey.
                  if candidate.from_date > journey[-1].to_date
                  # Select flight leaving from the country that last flight in journey arrived.
                  and candidate.from_country == journey[-1].to_country
                  and candidate.to_country != journey[-1].from_country
                  # Select flights that arrive in country from which the journey began.
                  and candidate.to_country == journey[0].from_country
                  # Select flights that arrive sooner then year (+-24 hours) after first flight.
                  and relativedelta(candidate.to_date - timedelta(days=1),
                                    journey[0].from_date).years < 1]

    return [extend(journey, candidate) for candidate in candidates]


def extend(journey, candidate):
    """Creates new list by extends journey with candidate. """
    next_journey = list(journey)
    next_journey.append(candidate)
    return next_journey


def filter_candidates(journey, list_of_flights):
    """List of all possible flights is filtered to only valid candidates."""
    candidates = [candidate for candidate in list_of_flights
                  # Select flights departing after arrival of last flight in the journey.
                  if candidate.from_date > journey[-1].to_date
                  # Select flights that arrive in country that was not visited on the journey yet.
                  and candidate.to_country not in [flight.from_country for flight in journey]
                  # Select flight leaving from the country that last flight in journey arrived.
                  and candidate.from_country == journey[-1].to_country
                  and candidate.to_country != journey[-1].from_country
                  # Select flights that arrive sooner then year (+-24 hours) after first flight.
                  and relativedelta(candidate.to_date - timedelta(days=1),
                                    journey[0].from_date).years < 1]

    # to reduce complexity of recursion
    return down_sample(candidates, NUMBER_OF_CANDIDATES_PER_SEARCH)


def filter_out_invalid_flights(flights):
    """Flights that start and end in the same country are no valid."""
    print("original size: %s" % len(flights))
    valid_flights = [flight for flight in flights
                     if flight.from_country != flight.to_country]
    print("filtered size: %s" % len(valid_flights))
    return valid_flights


def down_sample(list_input, k):
    """This function down samples elements in list."""
    length = len(list_input)
    if length < k:
        return list_input
    else:
        # down sample list_input to k equidistant items
        return [list_input[i] for i in range(0, int(length/k)*k, int(length/k))]



def get_list_of_airports():
    """get list of airports from input data file."""
    list_of_airports = []
    with open(CSV_INPUT, 'r') as csv_file:
        flights_reader = csv.reader(csv_file, delimiter=";")
        next(flights_reader, None)  # skip header
        for row in flights_reader:
            if row[0] not in list_of_airports:  # origin airports
                list_of_airports.append(row[0])

            if row[1] not in list_of_airports:  # destination airports
                list_of_airports.append(row[1])
    return list_of_airports


def get_list_of_flights():
    """creates list of flights"""
    list_of_flights = []
    with open(CSV_INPUT, 'r') as csv_file:
        flights_reader = csv.reader(csv_file, delimiter=";")
        next(flights_reader, None)  # skip header
        for row in flights_reader:
            try:
                list_of_flights.append(construct_flight_data(row))
            except KeyError:
                pass

    return list_of_flights


def get_dictionary_of_airports(list_of_airports, size_of_pool):
    """Creates dictionary containing 'airport code' : 'country' pairs.

    Because the scrapping from web takes a long time. The result is saved
    to file and read on the next try.
    """
    print("Gathering information about %s airports" % len(list_of_airports))
    if not os.path.isfile("airport_names.txt"):
        thread_pool = ThreadPool(size_of_pool)
        thread_pool.map(get_airport_country, list_of_airports)
        thread_pool.wait_completion()
        airport_list_text = ""
        for airport, country in dictionary_of_airports.items():
            airport_list_text += "%s:%s\n" % (airport, country)

        with open("airport_names.txt", "w") as airports_file:
            airports_file.write(airport_list_text)
    else:
        with open("airport_names.txt", "r") as airports_file:
            for line in airports_file:
                airport, country = line[:-1].split(":")
                dictionary_of_airports[airport] = country


def get_airport_country(airport):
    """Fetch airport info from www.world-airport-codes.com."""
    req = Request(compose_request(airport), headers={'User-Agent': "kiwi project"})
    response = send_request(req, airport)
    country_code = parse_country_code(response, airport)
    if country_code:
        with lock:
            dictionary_of_airports[airport] = country_code
    else:
        print("Couldn't find country for '%s' airport" % airport)


def send_request(req, airport):
    """Function tries to send request 3 times before it moves on"""
    tries = 0
    response = ""
    while tries < NUMBER_OF_RETRIES:
        try:
            response = urlopen(req).read()
            break
        except URLError as error:
            print("Error during %s try of sending request for airport '%s': %s" % (
                tries, airport, error))
            tries += 1
            response = ""
            sleep(2)
    return response


def compose_request(airport):
    """compose string for search request"""
    if len(airport) != 3:
        raise Exception("Airport key '%s' is not valid" % airport)
    return "{}search/?s={}".format(ADDRESS, airport.lower())


def parse_country_code(response, airport):
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
    except Exception as error:
        print("Error during parsing of response for airport '%s': %s" % (
            airport, error))
        code = None
    return code


def length_of_journey(journey):
    """Helper function for sorting flights by its length."""
    return journey[-1].to_date - journey[0].from_date


def main(size_of_pool):
    """main function"""
    airports = get_list_of_airports()

    get_dictionary_of_airports(airports, size_of_pool)

    flights = sorted(filter_out_invalid_flights(get_list_of_flights()),
                     key=operator.attrgetter('from_date'))

    results = analyze_routes(flights)

    proper_journeys = []

    for list_of_journeys in results:
        if len(list_of_journeys) > 0:
            for journey in list_of_journeys:
                proper_journeys.append(journey)

    journeys = sorted(proper_journeys,
                      key=length_of_journey)

    write_journeys_to_file(journeys)


lock = RLock()
dictionary_of_airports = {}

# if __name__ == "__main__":
main(50)
