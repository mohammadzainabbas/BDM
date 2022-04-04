from os.path import join
from utils import get_today_date, get_parent, fetch_data

"""

@todos: Implement the following in next stage/step

1. Scrape the omio website to extract the information.
2. Render search results with destination as BCN. And extract the trains' information. (maybe need to implement this in NodeJS due to bot testing with different User-Agent in request)
3. Add manual delays to avoid bot detection (time wise)

"""

def main():
    url = "https://www.omio.com/GoEuroAPI/rest/api/v5/results?direction=outbound&easy=0&enable_rn_refund_guarantee_tcp=false&exclude_offsite_bus_results=true&exclude_offsite_train_results=true&flight_default_sorting_ab_test=false&include_segment_positions=true&search_id=LC2F7EB5CFA8F4A87A7F85C9ED86A5E5E&sort_by=updateTime&sort_variants=outboundDepartureTime,outboundPrice,price,smart,traveltime&updated_since=0&use_recommendation=true&use_stats=true"
    activity_type = "omio_train"
    today_date = get_today_date()
    path = get_parent(join(today_date, activity_type))
    data = fetch_data(url, verbose=True, raw=True)
    print(data)
    # records = data['result']['records']
    # file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    # json_to_csv(records, file_path)
    # print_log("Fetched {} records on {} for '{}' from url '{}' and saved to '{}'".format(len(records), today_date, activity_type, url, file_path))

if __name__ == '__main__':
    main()