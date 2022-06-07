from os.path import join, abspath, pardir, dirname
from sys import path
parent_dir = abspath(join(dirname(abspath(__file__)), pardir))
path.append(parent_dir)
from common.utils import *

def get_parent(par_dir):
    prefix = join(abspath(join(parent_dir, pardir)), "data") 
    path = join(prefix, "events", par_dir)
    create_if_not_exists(path)
    return path