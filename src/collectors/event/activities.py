from functools import total_ordering
from os.path import join
from utils import get_today_date, get_parent, fetch_data, json_to_csv, print_log

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"
START_URL = "/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"

def parse_schema(fields):
    return fields

def get_activities():
    data = list()
    schema = list()
    is_first = True
    total_data = 1000
    while(total_data > 0):
        _result_ = fetch_data("{}{}".format(BASE_URL, START_URL), verbose=True)
        _data_ = _result_['result']['records']
        data.extend(_result_['result']['records'])
        if is_first:
            total_data = _result_['result']['total']
            schema = parse_schema(_result_['result']['fields'])
            is_first = False
        total_data = total_data - len(_data_)
        if _result_['_links']['next']
    return data, schema

https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c
https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?offset=100&resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c


def main():
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"
    activity_type = "activities"
    today_date = get_today_date()
    path = get_parent(join(today_date, activity_type))
    data = fetch_data(url, verbose=True)
    records = data['result']['records']
    file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    json_to_csv(records, file_path)
    print_log("Fetched {} records on {} for '{}' from url '{}' and saved to '{}'".format(len(records), today_date, activity_type, url, file_path))

if __name__ == '__main__':
    main()