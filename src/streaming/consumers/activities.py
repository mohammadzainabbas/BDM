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
    r"""
    
    Offical documentation for streaming: https://spark.apache.org/docs/latest/streaming-programming-guide.html

    """
    # For HDFS
    HDFS_DEFAULT = "hdfs://alakazam.fib.upc.es:27000"
    HDFS_USER = "bdm"
    HDFS_HOME = "/user/{}".format(HDFS_USER)

    # For HDFS Path
    hdfs_home = "{}{}".format(HDFS_DEFAULT, HDFS_HOME)

    __hdfs_location = "{}/{}".format(hdfs_home, join("formatted_data", "activities"))
    
    # For schema (schema won't be changed)
    data_date = "20220404"

    activities_file = "{}/{}/{}".format(hdfs_home, activities_dir, "activities_{}.parquet".format(data_date))

    # Get spark streaming session
    spark = get_streaming_spark_session()

    # Read from Kafka stream
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_server_host_n_port()) \
        .option("subscribe", get_kafka_topic()) \
        .option("failOnDataLoss","false") \
        .load()

    # 
    
    
def main() -> None:

    config = get_kafka_consumer_config() # Get all configurations for Kafka consumer
    stream_name = get_kafka_topic() # name of the stream
    
    consumer = KafkaConsumer(stream_name, **config)
    get_activities_from_stream(consumer)

if __name__ == '__main__':
    main()
