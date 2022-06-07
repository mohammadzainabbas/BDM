from os.path import join
from json import dumps, loads
from utils import get_today_date, get_parent, fetch_data, print_log, get_kafka_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaProducer

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"
START_URL = "/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"

def fetch_all_activities() -> list:
    """
    Repeatedly call the API endpoint and fetch all records 
    """
    data, is_first, total_data, start_url = list(), True, 0, START_URL
    # Fetch till we have all the records (a parameter 'total' in the API call)
    while(True):
        _result_ = defaultdict(lambda: None, fetch_data("{}{}".format(BASE_URL, start_url), verbose=True) )
        _data_ = _result_['result']['records']
        data.extend(_data_)
        if is_first:
            total_data = _result_['result']['total']
            is_first = False
        total_data = total_data - len(_data_)
        if total_data == 0: break
        start_url = _result_['result']['_links']['next']
        if not start_url: break
    return data

def say_hello():
    test_data = { 'name': 'Mohammad', 'age': 27, 'x': -5.12, 'y': 34.48 }
    print(test_data)
    config = get_kafka_config()
    server = KafkaProducer(**config)
    # stream = get_kafka_topic()
    # server.send(stream, value="Hello World")
    
def main():
    
    say_hello()

    # print(get_kafka_config())

    # activity_type = "activities"
    # today_date = get_today_date()
    # path = get_parent(join(today_date, activity_type))
    # data = get_activities()
    # file_path = join(path, '{}_{}.csv'.format(activity_type, today_date))
    # json_to_csv(data, file_path)
    # print_log("Fetched {} records on {} for '{}' from base url '{}' and saved to '{}'".format(len(data), today_date, activity_type, BASE_URL, file_path))

if __name__ == '__main__':
    main()