from os.path import join
from json import dumps, loads
from utils import get_today_date, get_parent, fetch_data, print_log, get_kafka_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaConsumer

def get_hello():
    test_data = { 'name': 'Mohammad', 'age': 27, 'x': -5.12, 'y': 34.48 }
    print(test_data)
    config = get_kafka_config()
    stream = get_kafka_topic()
    consumer = KafkaConsumer(topics=stream, **config)
    consumer.send(stream, value=dumps(test_data, indent=2).encode('utf-8'))
    
def main():
    get_hello()    

if __name__ == '__main__':
    main()