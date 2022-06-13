from os.path import join
from pyspark.sql import SparkSession, SQLContext, functions as SF
from pyspark import SQLContext
from pyspark.sql.types import StringType
from random import choice
from functools import reduce
import names
from utils import get_hdfs_home, print_log, print_error

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
    """
    Since, we don't have the users data, so, we decided to generate the users data first in order to train some model
    """

    #=======================
    # Configurations
    #=======================

    # max users
    max_users = 500
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

    #=======================
    # Pre-process
    #=======================
 
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

    #=====================================================================
    # Combine all dfs and add users with random sampling
    #=====================================================================

    # concat all dfs
    df = concat_dataframes([df_activities, df_culture, df_tourist_points])

    # generate random users and add them as a new column 'user'
    generate_random = SF.udf(lambda : choice(users), StringType())
    df = df.withColumn("user", generate_random())

    # filter out columns which we don't need
    cols = ["user", "type", "name", "register_id"]
    df = df.select(cols)

    # save the data for users
    __count = df.count()
    if __count > 0: # don't store empty dataframe
        print_log("Wrote {} user records at '{}' as parquet file".format(__count, hdfs_location))
        df.write.mode("append").parquet(hdfs_location)
    else:
        print_error("No user record(s) to store")

if __name__ == '__main__':
    main()