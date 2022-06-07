from utils import fetch_data, get_kafka_producer_config, get_kafka_topic, send_data_as_stream
from collections import defaultdict
from kafka import KafkaProducer
from time import sleep

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"
START_URL = "/api/action/datastore_search?resource_id=3abb2414-1ee0-446e-9c25-380e938adb73"

def get_total() -> int:
    _result_ = defaultdict(lambda: None, fetch_data("{}{}".format(BASE_URL, START_URL), verbose=False) )
    _total_ = _result_['result']['total']
    return int(_total_ if _total_ else 0)

def fetch_all_cultural_events() -> list:
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

def get_cultural_events(server: KafkaProducer, stream_name: str):
    __total = 0
    __timer = 2 * 60 # check changes in data after every 2 min
    
    # Continuously get the data from the API
    while(True):
        _total_ = get_total() # Get the total no. of records

        # if no. of records are updated -> fetch new records and push them in stream
        if __total != _total_ and _total_ != 0:
            __data = fetch_all_cultural_events()
            __total = _total_
            send_data_as_stream(__data, server, stream_name)
        else:
            sleep(__timer)

def main():
    
    config = get_kafka_producer_config() # Get all configurations for Kafka producer
    stream_name = get_kafka_topic() # name of the stream
    
    server = KafkaProducer(**config)
    get_cultural_events(server, stream_name)

if __name__ == '__main__':
    main()