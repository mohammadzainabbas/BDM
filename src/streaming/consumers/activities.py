import imp
from os.path import join
from json import dumps, loads
from streaming.common.utils import print_error
from utils import get_today_date, get_kafka_topic, get_kafka_bootstrap_server_host_n_port, store_streaming_data_in_hdfs, print_log, get_kafka_consumer_config, get_kafka_topic, get_streaming_spark_session
from collections import defaultdict
from kafka import KafkaConsumer
from inspect import stack

from pyspark.sql import functions as SF
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,StringType, FloatType, ArrayType,IntegerType,TimestampType, LongType, BinaryType, MapType

def get_activities_from_stream(consumer: KafkaConsumer) -> None:
    __push_after_items = 10000 # No. of messages after which you have to store the data in HDFS  
    __data = list()
    __hdfs_location = None
    __format = 'parquet'

    spark = get_streaming_spark_session()
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_server_host_n_port()) \
        .option("subscribe", get_kafka_topic()) \
        .option("failOnDataLoss","false") \
        .load()
    
    

    
def main() -> None:

    config = get_kafka_consumer_config() # Get all configurations for Kafka consumer
    stream_name = get_kafka_topic() # name of the stream
    
    consumer = KafkaConsumer(stream_name, **config)
    get_activities_from_stream(consumer)

if __name__ == '__main__':
    main()
