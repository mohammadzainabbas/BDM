from os.path import join, abspath, pardir, dirname
from sys import path

from traitlets import Any
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *
from kafka import KafkaProducer
import pandas as pd

def get_parent(par_dir):
    prefix = join(abspath(join(parent_dir, pardir)), "data") 
    path = join(prefix, "events", par_dir)
    create_if_not_exists(path)
    return path

def send_list_data_as_stream(records: list, server: KafkaProducer, stream_name: str) -> None:
    for record in records:
        server.send(stream_name, value=record)

def send_data_as_stream(records, server: KafkaProducer, stream_name: str) -> None:
    if isinstance(records, list):
        send_list_data_as_stream(records, server, stream_name)
    elif isinstance(records, pd.DataFrame)
    for record in records:
        server.send(stream_name, value=record)
