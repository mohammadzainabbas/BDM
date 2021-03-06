from os.path import join
from utils import get_kafka_bootstrap_server_host_n_port, print_error, print_log, get_streaming_spark_session

from pyspark.sql import functions as SF
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, TimestampType, LongType

import warnings
warnings.filterwarnings("ignore") # disable warnings

STREAM_NAME = "cultural_events"

def get_cultural_events_data_schema() -> StructType:
    """
    Return the data schema for cultural_events
    """
    return StructType([
        StructField("addresses_roadtype_name", IntegerType(), True),
        StructField("addresses_end_street_number", LongType(), True),
        StructField("values_attribute_name", StringType(), True),
        StructField("addresses_road_name", StringType(), True),
        StructField("values_category", StringType(), True),
        StructField("addresses_zip_code", LongType(), True),
        StructField("secondary_filters_id", LongType(), True),
        StructField("values_value", StringType(), True),
        StructField("addresses_town", StringType(), True),
        StructField("geo_epgs_4326_y", DoubleType(), True),
        StructField("geo_epgs_4326_x", DoubleType(), True),
        StructField("secondary_filters_name", StringType(), True),
        StructField("secondary_filters_tree", LongType(), True),
        StructField("addresses_district_name", StringType(), True),
        StructField("geo_epgs_25831_x", DoubleType(), True),
        StructField("addresses_start_street_number", LongType(), True),
        StructField("register_id", StringType(), True),
        StructField("institution_id", LongType(), True),
        StructField("addresses_main_address", BooleanType(), True),
        StructField("addresses_district_id", LongType(), True),
        StructField("addresses_roadtype_id", IntegerType(), True),
        StructField("addresses_type", IntegerType(), True),
        StructField("addresses_neighborhood_id", LongType(), True),
        StructField("_id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("addresses_road_id", LongType(), True),
        StructField("created", TimestampType(), True),
        StructField("geo_epgs_25831_y", DoubleType(), True),
        StructField("institution_name", StringType(), True),
        StructField("modified", TimestampType(), True),
        StructField("secondary_filters_asia_id", LongType(), True),
        StructField("secondary_filters_fullpath", StringType(), True),
        StructField("values_description", StringType(), True),
        StructField("values_id", LongType(), True),
        StructField("addresses_neighborhood_name", StringType(), True),
        StructField("values_outstanding", BooleanType(), True),
        StructField("values_attribute_id", LongType(), True)
    ])

def get_api_cultural_events_data_schema() -> StructType:
    """
    Return the data schema for cultural_events coming from the API calls

    https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=877ccf66-9106-4ae2-be51-95a9f6469e4c
    """
    return StructType([
        StructField("_id", LongType(), True),
        StructField("addresses_roadtype_name", StringType(), True),
        StructField("addresses_end_street_number", StringType(), True),
        StructField("values_attribute_name", StringType(), True),
        StructField("addresses_road_name", StringType(), True),
        StructField("values_category", StringType(), True),
        StructField("addresses_zip_code", StringType(), True),
        StructField("secondary_filters_id", StringType(), True),
        StructField("values_value", StringType(), True),
        StructField("addresses_town", StringType(), True),
        StructField("geo_epgs_4326_y", StringType(), True),
        StructField("geo_epgs_4326_x", StringType(), True),
        StructField("secondary_filters_name", StringType(), True),
        StructField("secondary_filters_tree", StringType(), True),
        StructField("addresses_district_name", StringType(), True),
        StructField("geo_epgs_25831_x", StringType(), True),
        StructField("addresses_start_street_number", StringType(), True),
        StructField("register_id", StringType(), True),
        StructField("institution_id", StringType(), True),
        StructField("addresses_main_address", StringType(), True),
        StructField("addresses_district_id", StringType(), True),
        StructField("addresses_roadtype_id", StringType(), True),
        StructField("addresses_type", StringType(), True),
        StructField("addresses_neighborhood_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("addresses_road_id", StringType(), True),
        StructField("created", StringType(), True),
        StructField("geo_epgs_25831_y", StringType(), True),
        StructField("institution_name", StringType(), True),
        StructField("modified", StringType(), True),
        StructField("secondary_filters_asia_id", StringType(), True),
        StructField("secondary_filters_fullpath", StringType(), True),
        StructField("values_description", StringType(), True),
        StructField("values_id", StringType(), True),
        StructField("addresses_neighborhood_name", StringType(), True),
        StructField("values_outstanding", StringType(), True),
        StructField("values_attribute_id", StringType(), True)
    ])

def remove_missing_data(df, cols):
    """
    Remove missing data
    """
    for col in cols:
        df = df.filter(SF.col(col).isNotNull())
    return df

def update_schema(df, new_schema, list_cols):
    """
    Update the schema of the dataframe
    """
    cols = [col.name for col in df.schema.fields]
    for col in list_cols:
        if col in cols:
            __type = [item.dataType for item in new_schema if item.name == col]
            if not isinstance(__type, list): continue
            __type = __type[0]
            df = df.withColumn("{}_new".format(col), df[col].cast(__type)).drop(col)
            df = df.withColumn(col, df["{}_new".format(col)]).drop("{}_new".format(col))
            # df = df(col).cast(__type)
    return df

def required_columns() -> list:
    """
    List of required columns
    """
    return [
        'register_id', 'name', 'geo_epgs_4326_x', 'geo_epgs_4326_y', # Must
        'addresses_neighborhood_id', 'addresses_neighborhood_name', # For neighborhood's query
        'addresses_district_id', 'addresses_district_name', # For district query
        'addresses_road_name', 'addresses_road_id', # Maybe useful to search events on that road
        'timestamp' # For time keeping
    ]

def save_stream_in_hdfs(batch_df, batch_id, hdfs_location):
    """
    Save the stream in HDFS as parquet files
    """
    __count = batch_df.count()
    print_log("Batch ID: {}".format(batch_id))
    if __count > 0: # don't store empty dataframes (in case we don't have any stream at that moment)
        batch_df.write.mode('append').parquet(hdfs_location)
        print_log("Wrote {} records at '{}' as parquet file".format(__count, hdfs_location))
        # batch_df.write.mode('append').csv(hdfs_location)
    else:
        print_error("No data to store")
    # batch_df.printSchema()
    # batch_df.show(10)
    print("\n============================================\n")

def parse_value_from_string(x):
    """
    Decode byte-type to string
    """
    return x.decode('utf-8')

def get_cultural_events_from_stream() -> None:
    r"""
    
    Offical documentation for streaming: https://spark.apache.org/docs/latest/streaming-programming-guide.html

    """
    # For HDFS
    HDFS_DEFAULT = "hdfs://alakazam.fib.upc.es:27000"
    HDFS_USER = "bdm"
    HDFS_HOME = "/user/{}".format(HDFS_USER)

    # For HDFS Path
    hdfs_home = "{}{}".format(HDFS_DEFAULT, HDFS_HOME)

    # __hdfs_location = "{}/{}".format(hdfs_home, join("formatted_data", "cultural_events"))
    __hdfs_location = "{}/{}".format(hdfs_home, join("formatted_data", "cultural_events"))

    # Get spark streaming session
    spark = get_streaming_spark_session()

    # Read from Kafka stream
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", get_kafka_bootstrap_server_host_n_port()) \
        .option("subscribe", STREAM_NAME) \
        .option("failOnDataLoss","false") \
        .load()

    # schema which is coming from the API
    __schema = get_api_cultural_events_data_schema()

    # parse values by decoding byte-arrays
    binary_to_str = SF.udf(parse_value_from_string, StringType())

    # formatted values
    __df = df.withColumn("formatted_value", binary_to_str( SF.col("value")))

    # parse formatted value via "from_json" (https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.from_json.html)
    __df = __df.select(SF.from_json(SF.col("formatted_value"), __schema).alias("cultural_events_records"), "timestamp")
    __df = __df.select("cultural_events_records.*", "timestamp")
    
    # required columns
    __columns = required_columns()

    # update the schema (we can't do this in streaming sources)
    # __new_schema = get_cultural_events_data_schema()
    # __df = spark.createDataFrame(__df.rdd, __new_schema)
    # __df.printSchema()

    # remove missing values
    __df = remove_missing_data(__df, __columns[0:4])

    # filter out un-neccessary columns
    __df = __df.select(__columns)

    # drop duplicates for 'register_id'
    __df = __df.withWatermark('timestamp', '10 minutes').dropDuplicates(subset=['register_id'])
    
    # write stream to console
    # __df.writeStream.format("console").start().awaitTermination()

    # write stream to a 'parquet' file in an 'append' mode
    # https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html
    __df.writeStream.foreachBatch(lambda batch_df, batch_id: save_stream_in_hdfs(batch_df, batch_id, __hdfs_location)).start(outputMode='append').awaitTermination()
    
def main() -> None:

    get_cultural_events_from_stream()

if __name__ == '__main__':
    main()
