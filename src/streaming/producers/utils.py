from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *
from traitlets import Any
from kafka import KafkaProducer
import pandas as pd

# Helper methods for Kafka Producer

def get_kafka_producer_config() -> dict:
    """
    Return configurations for Kafka producer
    
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    """
    __config = get_common_kafka_config()
    __config.update({
        "value_serializer": lambda m: json.dumps(m, indent=2).encode('utf-8'),
    })
    return __config

def send_list_data_as_stream(records: list, server: KafkaProducer, stream_name: str) -> None:
    total = len(records)
    print_log("Sending {} records as stream '{}' ...".format(total, stream_name))
    for record in records:
        server.send(stream_name, value=record)
    print_log("Sent {} records as stream '{}'".format(total, stream_name))

def send_dataframe_as_stream(df: pd.DataFrame, server: KafkaProducer, stream_name: str) -> None:
    records = df.T.to_dict().values() # https://stackoverflow.com/a/29815523/6390175https://stackoverflow.com/a/29815523/6390175
    send_list_data_as_stream(records, server, stream_name)

def send_data_as_stream(records: Any, server: KafkaProducer, stream_name: str, verbose: bool = False) -> None:
    if isinstance(records, list):
        send_list_data_as_stream(records, server, stream_name, verbose)
    elif isinstance(records, pd.DataFrame):
        send_dataframe_as_stream(records, server, stream_name, verbose)
    else:
        print_log("Sending raw data as stream ...")
        server.send(stream_name, value=records) # assumes 'records' is serializable
        print_log("Sent raw data as stream")

