from os import getenv
from os.path import join
from utils import get_today_date, get_parent, print_log
from amadeus import Client, ResponseError
from json import dump
from dotenv import load_dotenv
load_dotenv() 

"""

@todos: Implement the following in next stage/step

1. Scrape the skyscanner website to extract the information.
2. Render search results with destination as BCN. And extract the flights' information. (maybe need to implement this in NodeJS due to bot testing with different User-Agent in request)
3. Add manual delays to avoid bot detection (time wise)

"""

def main():

    try:

        amadeus = Client(
            client_id=getenv("SKYSCANNER_CLIENT_ID"),
            client_secret=getenv("SKYSCANNER_CLIENT_SECRET")
            )

        response = amadeus.shopping.flight_offers_search.get(
            originLocationCode='PAR',
            destinationLocationCode='BCN',
            departureDate=get_today_date('%Y-%m-%d'),
            adults=1)

        activity_type = "skyscanner_flights"
        today_date = get_today_date()
        path = get_parent(join(today_date, activity_type))
        file_path = join(path, '{}_{}.json'.format(activity_type, get_today_date('%Y%m%d-%H:%M:%S')))
        records = response.data
        with open(file_path, "w") as f:
            dump(records, f, indent=5)
        print_log("Fetched {} records on {} for '{}' and saved to '{}'".format(len(records), today_date, activity_type, file_path))

    except ResponseError as error: print(error)

if __name__ == '__main__':
    main()
    