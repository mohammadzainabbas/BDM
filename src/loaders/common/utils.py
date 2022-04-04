from os import makedirs
from os.path import exists
from datetime import datetime
import requests
import json
import csv
from constants import HDFS_ADDRESS, HDFS_HOME, HDFS_USER
from hdfs import InsecureClient
from hdfs.ext.avro import AvroWriter
from hdfs.ext.avro import AvroReader
import glob
import re
import shutil

def get_hdfs_address():
    return HDFS_ADDRESS

def get_hdfs_user():
    return HDFS_USER

def get_hdfs_user_home():
    return HDFS_HOME

# client = InsecureClient('http://10.4.41.44:9870', user='bdm')

def mkdirs_hdfs(dir_path):
    with InsecureClient(url=get_hdfs_address(), user=get_hdfs_user()) as client:
        client.makedirs(dir_path)
        return True

def save_as_avro_hdfs(file_path, parsed_data, schema):
    with InsecureClient(url=get_hdfs_address(), user=get_hdfs_user()) as client:
        with AvroWriter(client, file_path,schema = schema) as writer:
            for record in parsed_data:
                writer.write(record)
            return True

def json_to_hdfs(file_path, json_object):
    with InsecureClient(url=get_hdfs_address(), user=get_hdfs_user()) as client:
        with client.write("{}{}".format(get_hdfs_user_home(), file_path), encoding="utf-8",permission=777, overwrite=True) as writer:
            json.dump(json_object, writer, ensure_ascii=False)
            return True

def print_log(text):
    """
    Logger
    """
    print("[ log ] {}".format(text))

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
