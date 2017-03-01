#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

from threading import Thread, RLock
from queue import Queue
import csv
import re
import os
import operator
from collections import namedtuple
from urllib.request import Request, urlopen, URLError
from time import sleep
from datetime import datetime
from bs4 import BeautifulSoup
import ipdb

FlightTuple = namedtuple("FlightTuple", [
    'from_airport',
    'from_country',
    'from_date',
    'to_airport',
    'to_country',
    'to_date'
])

ADDRESS = "https://www.world-airport-codes.com/"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")
DT_INPUT_FORMAT = "%Y-%m-%d %H:%M:%S"
DT_OUTPUT_FORMAT = "%Y-%m-%dT%H:%M"
CSV_INPUT = "input_data_short.csv"

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


class ThreadPool:
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


class FlightTree(object):
    pass


def construct_flight_data(values):
    return FlightTuple(
        from_airport=values[0],
        from_country=dictionary_of_airports[values[0]],
        from_date=datetime.strptime(values[2], DT_INPUT_FORMAT),
        to_airport=values[1],
        to_country=dictionary_of_airports[values[1]],
        to_date=datetime.strptime(values[3], DT_INPUT_FORMAT))


def return_flight_data(flight_data):
    return "{};{};{};{};{}".format(
        flight_data.from_country,
        flight_data.from_airport,
        flight_data.to_airport,
        datetime.strftime(flight_data.from_date, DT_OUTPUT_FORMAT),
        datetime.strftime(flight_data.to_date, DT_OUTPUT_FORMAT))


def main(size_of_pool):
    """main function"""
    airports = get_list_of_airports()
    print("Gathering information about %s airports" % len(airports))
    if not os.path.isfile("airport_names.txt"):
        get_dictionary_of_airports(airports, size_of_pool)

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

    flights = sorted(get_list_of_flights(), key=operator.attrgetter('from_date'))

    analyze_routes(flights)



def analyze_routes(flights):
    """analyze all possible routes"""
    possible_journeys = []

    for flight in flights:
        journey = find_route(flight, flights, 1, flight.from_country)
        if journey:
            possible_journeys.append(journey)


def find_route(flight, list_of_flights, flight_index, origin):
    """investigate one potential route"""
    result = validate_journey(list_of_flights, flight_index, origin)
    if result:
        return result
    else:
        candidats_by_country = [candidat for candidat in list_of_flights
                                if candidat.from_country == flight.to_country
                                and candidat.to_country != flight.from_country]
        candidats = [candidat for candidat in candidats_by_country
                     if candidat.from_date > flight.arfrom_date]
        for candidat in candidats:
            journey = find_route(candidat, list_of_flights, flight_index + 1, origin)

    if journey:
        return journey.insert(0, flight)
    else:
        return []

    if candidats:
        print("for flight '%s' : %s\n" %(flight, candidats))


def validate_journey(list_of_flights, flight_index, origin):
    if flight_index < 5:
        return []
    else:
        candidats = [candidat for candidat in list_of_flights if candidat.to_country == origin]
        return candidats




def get_list_of_airports():
    """get list of airports from input data file."""
    list_of_airports = []
    with open(CSV_INPUT, 'r') as csvfile:
        flights_reader = csv.reader(csvfile, delimiter=";")
        next(flights_reader, None) # skip header
        for row in flights_reader:
            if row[0] not in list_of_airports: # orgin airports
                list_of_airports.append(row[0])

            if row[1] not in list_of_airports: # destination airports
                list_of_airports.append(row[1])
    return list_of_airports



def get_list_of_flights():
    """creates list of flights"""
    list_of_flights = []
    with open(CSV_INPUT, 'r') as csvfile:
        flights_reader = csv.reader(csvfile, delimiter=";")
        next(flights_reader, None) # skip header
        for row in flights_reader:
            list_of_flights.append(construct_flight_data(row))
    return list_of_flights


def get_dictionary_of_airports(list_of_airports, size_of_pool):
    """Creates dictionary containing 'airport code' : 'country' pairs."""
    thread_pool = ThreadPool(size_of_pool)
    thread_pool.map(get_airport_country, list_of_airports)
    thread_pool.wait_completion()


def get_airport_country(airport):
    """Fetch airport info from www.world-airport-codes.com."""
    req = Request(compose_request(airport), headers={'User-Agent' : "kiwi project"})
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
    while tries < 5:
        try:
            response = urlopen(req).read()
            break
        except URLError as error:
            print("Error during %s try of sending request for ariport '%s': %s" % (
                tries, airport, error))
            tries += 1
            response = ""
            sleep(2)
    return response


def compose_request(airport):
    """compose string for search request"""
    if len(airport) != 3:
        raise Exception("Airport key '%s' is not valid" % airport)
    request = "%ssearch/?s=%s" % (ADDRESS, airport.lower())
    return request


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

    except UnboundLocalError as error:
        code = None
    except IndexError as error:
        code = None
    except Exception as error:
        print("Error during parsing of response for airport '%s': %s" % (
            airport, error))
        code = None
    return code


lock = RLock()
dictionary_of_airports = {}

# if __name__ == "__main__":
main(50)
