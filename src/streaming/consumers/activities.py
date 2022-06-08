from os.path import join
from json import dumps, loads
from utils import get_today_date, get_parent, store_streaming_data_in_hdfs, print_log, get_kafka_consumer_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaConsumer

def get_activities_from_stream(consumer: KafkaConsumer) -> None:
    __push_after_items = 1000 # No. of messages after which you have to store the data in HDFS  
    __data = list()
    __hdfs_location = None
    __format = 'parquet'
    for message in consumer:
        value = message.value
        __data.append(value)
        if len(__data) == __push_after_items:
            store_streaming_data_in_hdfs(__data, __hdfs_location, __format)
            __data = list() # empty out the list
    
    
def get_hello():
    # test_data = { 'name': 'Mohammad', 'age': 27, 'x': -5.12, 'y': 34.48 }
    # print(test_data)
    config = get_kafka_consumer_config()
    stream = get_kafka_topic()
    consumer = KafkaConsumer(stream, **config)

    
def main() -> None:

    config = get_kafka_consumer_config() # Get all configurations for Kafka consumer
    stream_name = get_kafka_topic() # name of the stream
    
    consumer = KafkaConsumer(stream_name, **config)
    get_activities_from_stream(consumer)

if __name__ == '__main__':
    main()
