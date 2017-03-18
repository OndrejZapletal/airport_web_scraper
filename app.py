#!/usr/bin/env python3
"""Solution of kiwi code challenge."""

from journey_founder import analyze_routes
from output_processing import process_journeys
from scrapping import get_flight_information


def app():
    """main function"""
    flights = get_flight_information()
    journeys = analyze_routes(flights)
    process_journeys(journeys)


if __name__ == "__main__":
    app()
