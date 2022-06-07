from kafka import KafkaProducer
from ...collectors.event.activities import get_activities

def main():
    print(get_activities())
    print("GH")

if __name__ == "__main__":
    main()