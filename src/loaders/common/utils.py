from os import makedirs
from os.path import exists
from datetime import datetime
import pandas as pd
import glob, re, shutil, requests, json, csv
from pyspark.sql import SparkSession, session
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from hdfs import InsecureClient
from hdfs.util import HdfsError
# from hdfs.ext.avro import AvroWriter
# from hdfs.ext.avro import AvroReader
from .constants import HDFS_ADDRESS, HDFS_HOME, HDFS_USER

def get_hdfs_address():
    return HDFS_ADDRESS

def get_hdfs_user():
    return HDFS_USER

def get_hdfs_user_home():
    return HDFS_HOME

def get_hdfs_client():
    return InsecureClient(url=get_hdfs_address(), user=get_hdfs_user())

def get_spark_session() -> session.SparkSession:
    return SparkSession.builder.appName("bdm").getOrCreate()

def print_log(text):
    """
    Logger
    """
    print("[ log ] {}".format(text))

def print_error(text):
    """
    Logger for error
    """
    print("[ error ] {}".format(text))

# client = InsecureClient('http://10.4.41.44:9870', user='bdm')

def mkdirs_hdfs(dir_path):
    client = get_hdfs_client()
    client.makedirs(dir_path)
    return True

def save_as_avro_hdfs(file_path, parsed_data, schema):
    client = get_hdfs_client()
    with AvroWriter(client, file_path,schema = schema) as writer:
        for record in parsed_data:
            writer.write(record)
        return True

def save_df_as_parquet(file_path: str, df: pd.DataFrame) -> None:
    spark = get_spark_session()
    spark.createDataFrame(df).write.parquet(file_path)

def csv_to_parquet(file_path: str) -> tuple:
    """
    Save CSV file in parquet format

    Link: https://mungingdata.com/python/writing-parquet-pandas-pyspark-koalas/
    """
    _file_path = file_path.split('/')
    path, file_name = str("/".join(_file_path[:-1])), "".join(_file_path[-1].split(".")[:-1])
    file_name = "{}.parquet".format(file_name)
    full_path = '{}/{}'.format(path, file_name)
    pq.write_table( pcsv.read_csv(file_path) , full_path)
    return full_path, file_name

def json_to_hdfs(file_path, json_object):
    client = get_hdfs_client()
    with client.write("{}{}".format(get_hdfs_user_home(), file_path), encoding="utf-8",permission=777, overwrite=True) as writer:
        json.dump(json_object, writer, ensure_ascii=False)
        return True

def write_to_hdfs(hdfs_path, local_path):
    client = get_hdfs_client()
    try:
        client.upload(hdfs_path, local_path)
    except HdfsError as e:
        print_error("{}".format(e))

def json_to_csv(records, path):
    """
    Convert json and save as csv
    """
    keys = records[0].keys()
    with open(path, 'w') as outfile:
        writer = csv.DictWriter(outfile, keys)
        writer.writeheader()
        writer.writerows(records)

def fetch_data(url, verbose=False, raw=False, **kwargs):
    """
    Send a GET request to a given url
    """
    try:

        if verbose: print_log("Fetching data from: {}".format(url))
        result = requests.get(url, **kwargs)
        if raw: return result
        return json.loads(result.text)

    except Exception as e: print("[Error] {}".format(e))

def get_today_date(format='%Y%m%d'):
    """
    Get today's date (in formatted manner)
    """
    return datetime.today().strftime(format)

def create_if_not_exists(path):
    """
    Create dir if not exists
    """
    if not exists(path): makedirs(path)
