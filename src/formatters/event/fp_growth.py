from os.path import join
from pyspark.sql import SparkSession, SQLContext, functions as SF
from pyspark import SQLContext
from pyspark.ml.fpm import FPGrowth
from time import time

from utils import get_hdfs_client, get_hdfs_home, get_files, write_to_hdfs, print_log

def main():
    """
    Since, we don't have the users data, so, we decided to generate the users data and then build an FP growth model to get most frequent sets.
    This will help us in the future for recommending users to visit new places. (based on the association rules)
    
    In order to do that, we first generated users' data via 'formatters/event/generate_users.py' script and saved the users data in HDFS
    
    doc: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.fpm.FPGrowth.html
    """

    #=======================
    # Configurations
    #=======================

    parent_dir = "formatted_data"
    # For HDFS Path
    hdfs_home = get_hdfs_home()
    # For users
    users_dir = "{}/{}".format(hdfs_home, join(parent_dir, "users"))

    # For model
    model_location = "{}/{}".format(hdfs_home, join(parent_dir, "model"))

    #=======================
    # Spark settings
    #=======================
 
    spark = SparkSession.builder.appName("bdm").master('local').getOrCreate()
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    #=======================
    # Pre-process
    #=======================

    files = client.list(users_dir)
    if not len(files):
        # no files
        print("No users")
    else:
        df = sqlContext.read.parquet(hdfs_location)
        df.show(10)

    # read the data from users location
    df = sqlContext.read.parquet(users_dir)

    # get all users and places where they have visited
    df = df.groupBy("user").agg(SF.collect_list("register_id").alias("items"))

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

    # save FPGrowth model
    model.save(model_location)

    # transform examines the input items against all the association rules and summarize the
    # consequents as prediction
    # model.transform(df).show()

if __name__ == '__main__':
    main()