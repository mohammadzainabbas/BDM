from os.path import join
from pyspark.sql import SparkSession, SQLContext, functions as SF
from pyspark import SparkContext, SQLContext
import names
from functools import reduce
from utils import get_hdfs_client, get_hdfs_home, get_files, write_to_hdfs, print_log

activities_cols = ['register_id', 'name', 'geo_epgs_4326_x', 'geo_epgs_4326_y', # Must
    'addresses_neighborhood_id', 'addresses_neighborhood_name', # For neighborhood's query
    'addresses_district_id', 'addresses_district_name', # For district query
    'addresses_road_name', 'addresses_road_id' # Maybe useful to search events on that road
]

def main():

    # For HDFS Path

    hdfs_home = "{}{}".format(HDFS_DEFAULT, HDFS_HOME)

    # For users
    users_dir = join("formatted_data", "users")
    hdfs_location = "{}/{}".format(hdfs_home, users_dir)

    # For events
    activities_dir = "{}/{}".format(hdfs_home, join("formatted_data", "activities"))
    culture_dir = "{}/{}".format(hdfs_home, join("formatted_data", "cultural_events"))
    tourist_points_dir = "{}/{}".format(hdfs_home, join("formatted_data", "touristic_points"))


    activity_type = "activities"
    files, prefix = get_files(activity_type)
    print_log("Loading {} files of '{}' into HDFS".format(len(files), activity_type))
    hdfs_path = "{}/{}/{}".format(get_hdfs_user_home(), prefix, activity_type)
    # for file in files:
    #     file_name = str(file).split("/")[-1]
    #     print_log("File '{}' moved to HDFS at '{}'".format(file, hdfs_path))
    #     write_to_hdfs(join(hdfs_path, file_name), file)
    print_log("Saved {} file(s) of '{}' into HDFS at '{}'".format(len(files), activity_type, hdfs_path))

if __name__ == '__main__':
    main()