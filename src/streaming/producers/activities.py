from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(join(dirname(abspath(__file__)), pardir), pardir))
print(parent_dir)
path.append(join(parent_dir, "collectors"))
from collectors.event.activities import get_activities
# from kafka import KafkaProducer

def main():
    print(get_activities())
    print("GH")

if __name__ == "__main__":
    main()