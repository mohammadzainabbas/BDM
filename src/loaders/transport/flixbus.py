from os import getenv
from os.path import join
from utils import get_today_date, get_parent, fetch_data

"""

@todos: Implement the following in next stage/step

1. Fix the issue with API via Python or switch to NodeJS implementation. (https://github.com/juliuste/flix)
2. Else, find some alternative solution for this.

"""

def main():
    # url = "https://api.flixbus.com/mobile/v1/trip/search.json"
    url = "https://api.flixbus.com/mobile/v1/trip/search.json?departure_date=04.04.2022&search_by=cities&currency=EUR&from=Berlin&from_city_id=88&to_city_id=118&children=1&adults=1&bicycles=0&back=0"
    # url = "https://api.flixbus.com/mobile/v1/trip/search.json?departure_date=04.04.2022&search_by=cities&currency=EUR&from=Berlin&from_city_id=88&to_city_id=118&children=1&adults=1&bicycles=0&back=0"
    headers = {
        "X-API-Authentication": getenv("FLIXBUS_API_KEY") ,
        "User-Agent": "FlixBus/3.3 (iPhone; iOS 9.3.4; Scale/2.00)",
        "X-User-Country": "de",
    }
    params = {
        "departure_date":"04.04.2022",
        "search_by":"cities",
        "currency":"EUR",
        "from":"Berlin",
        "from_city_id":88,
        "to_city_id":118,
        "children":1,
        "adults":1,
        "bicycles":0,
        "back":0
        }
    activity_type = "flixbus"
    today_date = get_today_date()
    # path = get_parent(join(today_date, activity_type))
    data = fetch_data(url, verbose=True, raw=True, headers=headers, params=params)
    print(data)
    # records = data['result']['records']
    # file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    # json_to_csv(records, file_path)
    # print_log("Fetched {} records on {} for '{}' from url '{}' and saved to '{}'".format(len(records), today_date, activity_type, url, file_path))

if __name__ == '__main__':
    main()