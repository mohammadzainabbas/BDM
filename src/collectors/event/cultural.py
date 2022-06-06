from os.path import join
from utils import get_today_date, get_parent, fetch_data, json_to_csv, print_log

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"

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