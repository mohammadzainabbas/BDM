import imp
from os.path import join
from json import dumps, loads
from streaming.common.utils import print_error
from utils import get_today_date, get_parent, store_streaming_data_in_hdfs, print_log, get_kafka_consumer_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaConsumer
from inspect import stack

def get_activities_from_stream(consumer: KafkaConsumer) -> None:
    __push_after_items = 10000 # No. of messages after which you have to store the data in HDFS  
    __data = list()
    __hdfs_location = None
    __format = 'parquet'
    for message in consumer:
        value = message.value
        __data.append(value)
        if (len(__data) % __push_after_items) == 0:
            try:
                store_streaming_data_in_hdfs(__data, __hdfs_location, __format)
                __data = list() # empty out the list in 
            except Exception as e:
                __function_name = stack()[0][3] if stack()[0][3] else None # https://stackoverflow.com/a/55253296/6390175
                print_error("Something went wrong in {}\n{}".format(__function_name, e))
    
def main() -> None:

    config = get_kafka_consumer_config() # Get all configurations for Kafka consumer
    stream_name = get_kafka_topic() # name of the stream
    
    consumer = KafkaConsumer(stream_name, **config)
    get_activities_from_stream(consumer)

if __name__ == '__main__':
    main()
