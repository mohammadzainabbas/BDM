from os.path import join
from pyexpat import model

from pyspark.sql import SparkSession, SQLContext, functions as SF
from pyspark import SparkContext, SQLContext
from pyspark.sql.types import StringType
from pyspark.ml.fpm import FPGrowth
from time import time
from random import choice

import names
from functools import reduce
from utils import get_hdfs_client, get_hdfs_home, get_files, write_to_hdfs, print_log

def main():
    """
    Since, we don't have the users data, so, we decided to generate the users data and then build an FP growth model to get most frequent sets.
    This will help us in the future for recommending users to visit new places. (based on the association rules)
    
    In order to do that, we first generated users' data via 'formatters/event/generate_users.py' script and saved the users data in HDFS
    """

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

    # concat all dfs
    df = concat_dataframes([df_activities, df_culture, df_tourist_points])

    # generate random users and add them as a new column 'user'
    generate_random = SF.udf(lambda : choice(users), StringType())
    df = df.withColumn("user", generate_random())

    # filter out columns which we don't need
    cols = ["user", "type", "name", "register_id"]
    df = df.select(cols)

    # get all users and places where they have visited
    df = df.groupBy("user").agg(SF.collect_list("register_id").alias("items"))

    # for each type and name, get the count for how many times that place was visited
    # df1 = df.groupBy('type', 'name').agg(SF.count('name').alias('trip_count'))
    # df2 = df1.sort(df1.trip_count.desc()).show()

    #=======================
    # FP Growth
    #=======================

    data = df.select("items")

    fp = FPGrowth(minSupport=0.2, minConfidence=0.7)

    print_log("Fitting FPGrowth model ...")

    start_time = time()

    model = fp.fit(data)

    print_log("Took {} seconds to fit the data ...".format(time() - start_time))

    print_log("Showing most frequent itemset ...")

    # show most frequent itemsets.
    model.freqItemsets.show()

    print_log("\n===============\n")
    print_log("Showing generated association rules ...")

    # show generated association rules.
    model.associationRules.show()

    print_log("\n===============\n")
    print_log("Saving model at '{}' ...".format( model_location ))



    # transform examines the input items against all the association rules and summarize the
    # consequents as prediction
    # model.transform(df).show()

if __name__ == '__main__':
    main()