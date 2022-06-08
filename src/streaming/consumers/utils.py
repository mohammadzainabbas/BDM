from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *
from traitlets import Any
import pandas as pd

from pyspark.sql import SparkSession, session
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from hdfs import InsecureClient
from hdfs.util import HdfsError

# Spark Streaming

def get_spark_session() -> session.SparkSession:
    return SparkSession.builder.appName("bdm").getOrCreate()

def save_df_as_parquet(file_path: str, df: pd.DataFrame) -> None:
    spark = get_spark_session()
    spark.createDataFrame(df).write.parquet(file_path)

# Helper methods for Kafka consumer

def get_kafka_consumer_config() -> dict:
    """
    Return configurations for Kafka consumer
    
    Reference: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
    """
    __config = get_common_kafka_config()
    __config.update({
        "value_deserializer": lambda m: json.loads(m.decode('utf-8')),
    })
    return __config

def store_raw_data_in_hdfs(data: Any, hdfs_location: str, format: str = 'parquet') -> None:
    return None

def store_dataframe_data_in_hdfs(df: pd.DataFrame, hdfs_location: str, format: str = 'parquet') -> None:
    return None

def store_list_data_in_hdfs(data: list, hdfs_location: str, format: str = 'parquet') -> None:
    df = pd.DataFrame(data)
    store_dataframe_data_in_hdfs(df, hdfs_location, format)

def store_streaming_data_in_hdfs(data: Any, hdfs_location: str, format: str = 'parquet') -> None:
    if isinstance(data, list):
        store_list_data_in_hdfs(data, hdfs_location, format)
    elif isinstance(data, pd.DataFrame):
        store_dataframe_data_in_hdfs(data, hdfs_location, format)
    else:
        store_raw_data_in_hdfs(data, hdfs_location, format)
