#!/usr/bin/env python3

"""Solution of kiwi code challenge"""

from threading import Thread, RLock
from queue import Queue
import csv
import re
from urllib.request import Request, urlopen, URLError
from time import sleep
from bs4 import BeautifulSoup

ADDRESS = "https://www.world-airport-codes.com/"
COUNTRY_RE = re.compile(r".+\((\w\w)\).*")


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
                print(error)
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


def main(size_of_pool):
    """main function"""
    airports = get_list_of_airports()
    print("Gathering information about %s airports" % len(airports))
    get_dictionary_of_airports(airports, size_of_pool)

    airport_list_text = ""
    for airport, country in dictionary_of_airports.items():
        airport_list_text += "%s:%s\n" % (airport, country)

    with open("airport_names.txt", "w") as airports_file:
        airports_file.write(airport_list_text)


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
    return list_of_airports


def get_dictionary_of_airports(list_of_airports, size_of_pool):
    """Creates dictionary containing 'airport code' : 'country' pairs."""
    thread_pool = ThreadPool(size_of_pool)
    thread_pool.map(get_airport_country, list_of_airports)
    thread_pool.wait_completion()


def get_airport_country(airport_key):
    """Fetch airport info from www.world-airport-codes.com."""
    req = Request(compose_request(airport_key), headers={'User-Agent' : "kiwi project"})
    response = send_request(req)
    country_code = parse_country_code(response)
    if country_code:
        with lock:
            dictionary_of_airports[airport_key] = country_code
    else:
        print("Couldn't find country for '%s' airport" % airport_key)


def send_request(req):
    """Function tries to send request 3 times before it moves on"""
    tries = 0
    while tries < 5:
        try:
            response = urlopen(req).read()
            break
        except URLError as error:
            print("Error during %s try: %s" % (tries, error))
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


def parse_country_code(response):
    """returns code of country"""
    soup = BeautifulSoup(response, "html.parser")
    result = str(soup.find_all("div", "header clearfix")[0])
    soup = BeautifulSoup(result, "html.parser")
    result = str(soup.find("p").contents[0][1:-1])
    try:
        code = COUNTRY_RE.match(result).group(1)
    except AttributeError:
        code = None
    return code

lock = RLock()
dictionary_of_airports = {}

if __name__ == "__main__":
    main(100)

# with open("timing.txt", "w") as timing_file:
    # for i in range(5, 101, 5):
    #     start = time()
    #     for _ in range(10):
    #         main(i)
    #     timing_file.write("threads: %s, time: %s\n" % (i, time() - start))
