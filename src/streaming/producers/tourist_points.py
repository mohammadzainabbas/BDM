from utils import fetch_data, get_kafka_producer_config, send_data_as_stream, print_log
from collections import defaultdict
from kafka import KafkaProducer
from time import sleep

BASE_URL = "https://opendata-ajuntament.barcelona.cat/data"
START_URL = "/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c"
STREAM_NAME = "activities"

def get_total() -> int:
    _result_ = defaultdict(lambda: None, fetch_data("{}{}".format(BASE_URL, START_URL), verbose=False) )
    _total_ = _result_['result']['total']
    return int(_total_ if _total_ else 0)

def fetch_n_send_all_activities_as_stream(server: KafkaProducer, stream_name: str, verbose: bool = False) -> None:
    """
    Repeatedly call the API endpoint and fetch all records and send the result in a stream 
    """
    is_first, total_data, start_url = True, 0, START_URL
    # Fetch till we have all the records (a parameter 'total' in the API call)
    while(True):
        _result_ = defaultdict(lambda: None, fetch_data("{}{}".format(BASE_URL, start_url), verbose=verbose) )
        _data_ = _result_['result']['records']
        if verbose: print_log("Fetched {} records from the API '{}{}'".format( len(_data_), BASE_URL, start_url))
        send_data_as_stream(_data_, server, stream_name, verbose)
        if is_first:
            total_data = _result_['result']['total']
            is_first = False
        total_data = total_data - len(_data_)
        if total_data == 0: break
        start_url = _result_['result']['_links']['next']
        if not start_url: break

def get_activities(server: KafkaProducer, stream_name: str, verbose: bool = False) -> None:
    __total = 0
    __timer = 2 * 60 # check changes in data after every 2 min
    
    # Continuously get the data from the API
    while(True):
        if verbose: print_log("Checking API again to see if we have some change...")
        _total_ = get_total() # Get the total no. of records

        # if no. of records are updated -> fetch new records and push them in stream
        if __total != _total_ and _total_ != 0:
            if verbose: print_log("We have some change...")
            fetch_n_send_all_activities_as_stream(server, stream_name, verbose)
            __total = _total_
        else:
            if verbose: print_log("No change! Waiting to check API again after '{}' seconds ...".format(__timer))
            sleep(__timer)

def main() -> None:
    
    config = get_kafka_producer_config() # Get all configurations for Kafka producer
    stream_name = STREAM_NAME # name of the stream
    
    server = KafkaProducer(**config)
    get_activities(server, stream_name, verbose=True)

if __name__ == '__main__':
    main()
