from dataclasses import dataclass
from functools import total_ordering
from os.path import join
from utils import get_today_date, get_parent, fetch_data, json_to_csv, print_log

def parse_schema(fields):
    return fields

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"

def get_activities():
    START_URL = "/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"
    data = list()
    schema = list()
    is_first = True
    total_data = 0
    while(True):
        _result_ = fetch_data("{}{}".format(BASE_URL, START_URL), verbose=True)
        _data_ = _result_['result']['records']
        data.extend(_result_['result']['records'])
        if is_first:
            total_data = _result_['result']['total']
            schema = parse_schema(_result_['result']['fields'])
            is_first = False
        total_data = total_data - len(_data_)
        if total_data == 0:
            break
        START_URL = _result_['result']['_links']['next']
    return data, schema

def main():
    # url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"
    # activity_type = "activities"
    today_date = get_today_date()
    # path = get_parent(join(today_date, activity_type))
    # data = fetch_data(url, verbose=True)
    # records = data['result']['records']
    # file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    # json_to_csv(records, file_path)
    # print_log("Fetched {} records on {} for '{}' from url '{}' and saved to '{}'".format(len(records), today_date, activity_type, url, file_path))

    data, schema = get_activities()

    print("Total data: {}".format(len(data)))

if __name__ == '__main__':
    main()