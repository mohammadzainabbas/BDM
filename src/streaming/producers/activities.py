from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from ...collectors.event.activities import get_activities
from kafka import KafkaProducer

def main():
    print(get_activities())
    print("GH")

if __name__ == "__main__":
    main()