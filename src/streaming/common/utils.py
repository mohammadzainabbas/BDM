from os import makedirs
from os.path import exists
from datetime import datetime
import requests
import json
import csv

def print_log(text):
    """
    Logger
    """
    print("[ log ] {}".format(text))

def json_to_csv(records, path):
    """
    Convert json and save as csv
    """
    keys = records[0].keys()
    with open(path, 'w') as outfile:
        writer = csv.DictWriter(outfile, keys)
        writer.writeheader()
        writer.writerows(records)

def fetch_data(url, verbose=False, raw=False, **kwargs):
    """
    Send a GET request to a given url
    """
    try:

        if verbose: print_log("Fetching data from: {}".format(url))
        result = requests.get(url, **kwargs)
        if raw: return result
        return json.loads(result.text)

    except Exception as e: print("[Error] {}".format(e))

def get_today_date(format='%Y%m%d'):
    """
    Get today's date (in formatted manner)
    """
    return datetime.today().strftime(format)

def create_if_not_exists(path):
    """
    Create dir if not exists
    """
    if not exists(path): makedirs(path)

def get_common_kafka_config() -> dict:
    """
    Return common Kafka configurations
    """
    return {
        "bootstrap_servers": "localhost:9092"
    }

def get_kafka_producer_config() -> dict:
    """
    Return configurations for Kafka producer
    
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    """
    __config = get_common_kafka_config()
    return __config.update({
        "value_deserializer": lambda m: json.dumps(m, indent=2).encode('utf-8'),
    })

def get_kafka_consumer_config() -> dict:
    """
    Return configurations for Kafka consumer
    
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    """
    __config = get_common_kafka_config()
    __config.update({
        "value_deserializer": lambda m: json.loads(m.decode('utf-8')),
    })
    return __config

def get_kafka_topic() -> str:
    """
    Name of the Kafka topic/stream
    """
    # return "bcn_events"
    return "temp"