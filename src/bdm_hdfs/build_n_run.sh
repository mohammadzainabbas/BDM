#! /bin/bash

/home/bdm/apache-maven-3.8.5/bin/mvn package && java -cp /home/bdm/BDM/src/bdm_hdfs/target/bdm_hdfs-1.0-SNAPSHOT.jar com.upc.bdm_hdfs.App $@
