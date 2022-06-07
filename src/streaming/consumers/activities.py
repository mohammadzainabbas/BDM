from os.path import join
from json import dumps, loads
from utils import get_today_date, get_parent, fetch_data, print_log, get_kafka_config, get_kafka_topic
from collections import defaultdict
from kafka import KafkaConsumer

def get_hello():
    test_data = { 'name': 'Mohammad', 'age': 27, 'x': -5.12, 'y': 34.48 }
    print(test_data)
    config = get_kafka_config()
    server = KafkaConsumer(**config)
    stream = get_kafka_topic()
    server.send(stream, value=dumps(test_data, indent=2).encode('utf-8'))
    
def main():
    
    get_hello()

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