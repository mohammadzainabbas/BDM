from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *
from traitlets import Any
import pandas as pd

# Helper methods for Kafka consumer

def store_dataframe_data_in_hdfs(df: pd.DataFrame, hdfs_location: str, format: str = 'parquet') -> None:
    return None

def store_list_data_in_hdfs(data: list, hdfs_location: str, format: str = 'parquet') -> None:
    df = pd.DataFrame(data)
    store_dataframe_data_in_hdfs(df, hdfs_location, format)

def store_streaming_data_in_hdfs(data: Any, hdfs_location: str, format: str = 'parquet') -> None:
    if i




