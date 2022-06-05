from os import listdir, walk
from os.path import join, abspath, pardir, dirname, isfile
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *

def get_local_data_dir():
    return join(abspath(join(parent_dir, pardir)), "data")

def get_all_files(root):
    _files = list()
    for path, subdirs, files in walk(root):
        for file in files:
            _files.append(join(path, file))
    return _files

def get_files(type):
    prefix = join(get_local_data_dir(), "events")
    files = get_all_files(prefix)
    files = [x for x in files if x and isfile(x) and str(x).find(type) != -1]
    return files, join("data", "events")
