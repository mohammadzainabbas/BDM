from os.path import join
from json import dumps, loads
from utils import get_today_date, get_parent, fetch_data, print_log, get_kafka_consumer_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaConsumer

def get_activities_from_stream(consumer: KafkaConsumer) -> None:
    data = dict()
    for message in consumer:
        value = message.value


        print(type(message))
        print(message)
        print(type(value))
        print(value)
    
    
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
