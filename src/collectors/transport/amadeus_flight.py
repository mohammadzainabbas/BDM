from os import getenv
from os.path import join
from utils import get_today_date, get_parent, print_log
from amadeus import Client, ResponseError
from json import dump
from dotenv import load_dotenv
load_dotenv()

"""

@todos: Implement the following in next stage/step

1. Provide a list of origin airports codes and iteratatively call amadeus API and aggregate the data before saving
2. Save only data which is more relevant to our problem
3. Add sanity checks to keep track of API call limits (time wise)
4. In case API is failing, have some mechanism to detect that

"""

def main():

    try:

        amadeus = Client(
            client_id=getenv("AMADEUS_CLIENT_ID"),
            client_secret=getenv("AMADEUS_CLIENT_SECRET")
            )

        response = amadeus.shopping.flight_offers_search.get(
            originLocationCode='MAD',
            destinationLocationCode='BCN',
            departureDate=get_today_date('%Y-%m-%d'),
            adults=1)

        activity_type = "amadeus_flights"
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
