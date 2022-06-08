from os.path import join
from json import dumps, loads
from utils import get_today_date, get_kafka_topic, get_kafka_bootstrap_server_host_n_port, store_streaming_data_in_hdfs, print_log, get_kafka_consumer_config, get_kafka_topic, get_streaming_spark_session
from collections import defaultdict
from kafka import KafkaConsumer
from inspect import stack

from pyspark.sql import functions as SF
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import StructType,StructField,StringType, FloatType, ArrayType,IntegerType,TimestampType, LongType, BinaryType, MapType

def remove_missing_data(df, cols):
    for col in cols:
        df = df.filter(SF.col(col).isNotNull())
    return df

def required_columns() -> list:
    return [
        'register_id', 'name', 'geo_epgs_4326_x', 'geo_epgs_4326_y', # Must
        'addresses_neighborhood_id', 'addresses_neighborhood_name', # For neighborhood's query
        'addresses_district_id', 'addresses_district_name', # For district query
        'addresses_road_name', 'addresses_road_id', # Maybe useful to search events on that road
        'timestamp' # For time keeping
    ]

def save_stream_in_hdfs(batch_df, batch_id, hdfs_location):
    batch_df.write.mode('append').parquet(hdfs_location)
    # batch_df.write.mode('append').csv(hdfs_location)
    print_log("Batch ID: {}".format(batch_id))
    batch_df.printSchema()
    batch_df.show(10)
    print("\n============================================\n")

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

    # __hdfs_location = "{}/{}".format(hdfs_home, join("formatted_data", "activities"))
    __hdfs_location = "{}/{}".format(hdfs_home, join("formatted_data", "activities", "formatted_activities.parquet"))

    # Get spark streaming session
    spark = get_streaming_spark_session()

    # Read from Kafka stream
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_server_host_n_port()) \
        .option("subscribe", get_kafka_topic()) \
        .option("failOnDataLoss","false") \
        .load()

    # Since, schema won't be changed, we can get it from an old file
    # @todo: find a better way for schema (maybe save it somewhere to reuse it later)
    data_date = "20220404"
    activities_dir = join("data", "events", "activities")
    activities_file = "{}/{}/{}".format(hdfs_home, activities_dir, "activities_{}.parquet".format(data_date))
    df_activities = spark.read.format("parquet").load(activities_file)

    __schema = df_activities.schema # schema for activities

    __df = df.select(SF.from_json(df.value.cast("string"), __schema).alias("activities_records"), "timestamp")
    __df = __df.select("activities_records.*", "timestamp")
    __df.printSchema()
    
    # required columns
    __columns = required_columns()

    # remove missing values
    __df = remove_missing_data(__df, __columns[0:4])

    # filter out un-neccessary columns
    __df = __df.select(__columns)

    # drop duplicates for 'register_id'
    __df = __df.withWatermark('timestamp', '10 minutes').dropDuplicates(subset=['register_id'])

    # write stream to a 'parquet' file in an 'append' mode
    # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html
    __df.writeStream.foreachBatch(lambda batch_df, batch_id: save_stream_in_hdfs(batch_df, batch_id, __hdfs_location)).start(outputMode='append').awaitTermination()
    
def main() -> None:

    config = get_kafka_consumer_config() # Get all configurations for Kafka consumer
    stream_name = get_kafka_topic() # name of the stream
    
    consumer = KafkaConsumer(stream_name, **config)
    get_activities_from_stream(consumer)

if __name__ == '__main__':
    main()
