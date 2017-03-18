"""Jounery founder module"""

from concurrent.futures import ProcessPoolExecutor
from datetime import timedelta

from dateutil.relativedelta import relativedelta

LENGTH_OF_COMPLETE_JOURNEY = 10
NUMBER_OF_CANDIDATES_PER_SEARCH = 2
NUMBER_OF_JOURNEY_STARTS = 1000

def extend(journey, candidate):
    """Creates new list by extends journey with candidate. """
    next_journey = list(journey)
    next_journey.append(candidate)
    return next_journey


def analyze_routes(flights):
    """analyze possible routes"""
    # Creation of these lists is little bit convoluted by it is necessary for
    # serialization for ProcessPoolExecutor.
    flights = down_sample(flights, NUMBER_OF_JOURNEY_STARTS)
    journeys = [list([flight]) for flight in flights]
    flights_list = [flights for _ in range(len(flights))]
    args = list(zip(journeys, flights_list))

    with ProcessPoolExecutor() as executor:
        possible_journeys = list(executor.map(find_route, args))

    return possible_journeys


def down_sample(list_input, k):
    """This function down samples elements in list."""
    length = len(list_input)
    if length < k:
        return list_input
    else:
        # down sample list_input to k equidistant items
        return [
            list_input[i]
            for i in range(0, int(length / k) * k, int(length / k))
        ]


def find_route(args):
    """Recursive function investigates potential journey."""
    journey, flights = args
    if len(journey) == LENGTH_OF_COMPLETE_JOURNEY - 1:
        return validate_journey(journey, flights)
    else:
        journeys = []
        for candidate in filter_candidates(journey, flights):
            journeys += find_route((extend(journey, candidate), flights))
        return journeys


def validate_journey(journey, list_of_flights):
    """List of all possible flights is filtered to only valid candidates."""
    candidates = [
        candidate
        for candidate in list_of_flights
        # Select flights departing after arrival of last flight in the journey.
        if candidate.from_date > journey[-1].to_date
        # Select flight leaving from the country that last flight in journey arrived.
        and candidate.from_country == journey[-1].to_country
        # Select flights that arrive in country from which the journey began.
        and candidate.to_country == journey[0].from_country
        # Select flights that arrive sooner then year (+-24 hours) after first flight.
        and relativedelta(
            candidate.to_date - timedelta(days=1), journey[0].from_date).years
        < 1
    ]

    return [extend(journey, candidate) for candidate in candidates]


def filter_candidates(journey, list_of_flights):
    """List of all possible flights is filtered to only valid candidates."""
    candidates = [
        candidate
        for candidate in list_of_flights
        # Select flights departing after arrival of last flight in the journey.
        if candidate.from_date > journey[-1].to_date
        # Select flights that arrive in country that was not visited on the journey yet.
        and candidate.to_country not in
        [flight.from_country for flight in journey]
        # Select flight leaving from the country that last flight in journey arrived.
        and candidate.from_country == journey[-1].to_country
        # Select flights that arrive sooner then year (+-24 hours) after first flight.
        and relativedelta(
            candidate.to_date - timedelta(days=1), journey[0]
            .from_date).years < 1
    ]

    # to reduce complexity of recursion
    return down_sample(candidates, NUMBER_OF_CANDIDATES_PER_SEARCH)
