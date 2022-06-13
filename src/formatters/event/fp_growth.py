from os.path import join

from pyspark.sql import SparkSession, SQLContext, functions as SF
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StringType
from pyspark.ml.fpm import FPGrowth

from random import choice

import names
from functools import reduce
from utils import get_hdfs_client, get_hdfs_home, get_files, write_to_hdfs, print_log

activities_cols = ['register_id', 'name', 'geo_epgs_4326_x', 'geo_epgs_4326_y', # Must
    'addresses_neighborhood_id', 'addresses_neighborhood_name', # For neighborhood's query
    'addresses_district_id', 'addresses_district_name', # For district query
    'addresses_road_name', 'addresses_road_id' # Maybe useful to search events on that road
]

def concat_dataframes(dfs):
    """
    Concat multiple pyspark dataframe(s)

    https://www.geeksforgeeks.org/concatenate-two-pyspark-dataframes/
    """
    return reduce(lambda df1, df2: df1.union( df2.select( df1.columns ) ), dfs)

def get_random_name():
    """
    Generate random names
    """
    return names.get_full_name()

def get_random_users_list(users_count = 100):
    return [get_random_name() for i in range(0, users_count)]

def main():

    #=======================
    # Configurations
    #=======================

    # max users
    max_users = 100
    users = get_random_users_list(max_users)
 
    parent_dir = "formatted_data"
    # For HDFS Path
    hdfs_home = get_hdfs_home()
    # For users
    users_dir = join(parent_dir, "users")
    hdfs_location = "{}/{}".format(hdfs_home, users_dir)
    # For events
    activities_dir = "{}/{}".format(hdfs_home, join(parent_dir, "activities"))
    culture_dir = "{}/{}".format(hdfs_home, join(parent_dir, "cultural_events"))
    tourist_points_dir = "{}/{}".format(hdfs_home, join(parent_dir, "touristic_points"))

    #=======================
    # Spark settings
    #=======================
 
    spark = SparkSession.builder.appName("bdm").master('local').getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    df_activities = sqlContext.read.parquet(activities_dir)
    df_culture = sqlContext.read.parquet(culture_dir)
    df_tourist_points = sqlContext.read.parquet(tourist_points_dir)

    activities_type = SF.udf(lambda : "activities", StringType())
    culture_type = SF.udf(lambda : "cultural events", StringType())
    tourist_points_type = SF.udf(lambda : "tourist points", StringType())

    # add 'type' column
    df_activities = df_activities.withColumn("type", activities_type())
    df_culture = df_culture.withColumn("type", culture_type())
    df_tourist_points = df_tourist_points.withColumn("type", tourist_points_type())

    # concat all dfs
    df = concat_dataframes([df_activities, df_culture, df_tourist_points])

    # generate random users and add them as a new column 'user'
    generate_random = SF.udf(lambda : choice(users), StringType())
    df = df.withColumn("user", generate_random())

    # filter out columns which we don't need
    cols = ["user", "type", "name"]
    df = df.select(cols)

    # get all users and places where they have visited
    df = df.groupBy("user").agg(SF.collect_list("name"))

    #df.select(SF.collect_set("name").alias("name")).first()["name"]

    # for each type and name, get the count for how many times that place was visited
    # df1 = df.groupBy('type', 'name').agg(SF.count('name').alias('trip_count'))
    # df2 = df1.sort(df1.trip_count.desc()).show()

    



if __name__ == '__main__':
    main()