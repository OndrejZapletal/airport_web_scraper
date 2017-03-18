"""Utilities used in application"""

from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from pytz import country_timezones, timezone

AIRPORT_LIST_FILE = "airport_list.txt"
CSV_INPUT_FILE = "input_data.csv"
DT_OUTPUT_FORMAT = "%Y-%m-%dT%H:%M"
VALID_JOURNEYS_FILE = "valid_journeys.csv"

def process_journeys(results):
    """Process ouptput of journeys
    """
    journeys = []
    for list_of_journeys in results:
        if len(list_of_journeys) > 0:
            for journey in list_of_journeys:
                journeys.append(journey)

    journeys = sorted(journeys, key=length_of_journey)

    # saves found journeys into file
    write_journeys_to_file(journeys)

    # prints found journeys into file
    print(format_journey_data(journeys))


def length_of_journey(journey):
    """Helper function for sorting flights by its length."""
    return journey[-1].to_date - journey[0].from_date


def format_journey_data(list_of_journeys):
    """Create string containing all journeys data in printable format."""
    journeys_data = ""
    for index, journey in enumerate(list_of_journeys):
        for flight in journey:
            journeys_data += "%s;%s\n" % (index + 1,
                                          return_flight_data(flight))
    return journeys_data


def write_journeys_to_file(list_of_journeys):
    """Write found journeys into file."""
    with open(VALID_JOURNEYS_FILE, "w") as file:
        file.write(format_journey_data(list_of_journeys))


def get_local_date_time(datetime_input, country):
    """Convert UTC time into local time based on country. """
    local = timezone(country_timezones(country)[0])
    datetime_output = datetime_input.astimezone(local)
    return datetime.strftime(datetime_output, DT_OUTPUT_FORMAT)


def return_flight_data(flight_data):
    """format flight data into readable format."""
    return "{};{};{};{};{}".format(
        flight_data.from_country, flight_data.from_airport,
        flight_data.to_airport,
        get_local_date_time(flight_data.from_date, flight_data.from_country),
        get_local_date_time(flight_data.to_date, flight_data.to_country))


def filter_younger_then_year(journey, list_of_flights):
    """Function returns only flights that arrive before 1 year +- 24 hours
    from start of the journey.
    """
    return [
        candidate for candidate in list_of_flights
        if relativedelta(
            candidate.to_date - timedelta(days=1), journey[0].from_date).years
        < 1
    ]
