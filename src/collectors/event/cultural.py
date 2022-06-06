from os.path import join
from utils import get_today_date, get_parent, fetch_data, json_to_csv, print_log

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"

def get_cultural_events() -> list:
    start_url = "/api/action/datastore_search?resource_id=3abb2414-1ee0-446e-9c25-380e938adb73"
    data, is_first, total_data = list(), True, 0
    # Fetch till we have all the records (a parameter 'total' in the API call)
    while(True):
        _result_ = fetch_data("{}{}".format(BASE_URL, start_url), verbose=True)
        _data_ = _result_['result']['records']
        data.extend(_result_['result']['records'])
        if is_first:
            total_data = _result_['result']['total']
            is_first = False
        total_data = total_data - len(_data_)
        if total_data == 0: break
        start_url = _result_['result']['_links']['next']
    return data

def main():
    url = "https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=3abb2414-1ee0-446e-9c25-380e938adb73"
    activity_type = "culture"
    today_date = get_today_date()
    path = get_parent(join(today_date, activity_type))
    data = fetch_data(url, verbose=True)
    records = data['result']['records']
    file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    json_to_csv(records, file_path)
    print_log("Fetched {} records on {} for '{}' from url '{}' and saved to '{}'".format(len(records), today_date, activity_type, url, file_path))

if __name__ == '__main__':
    main()