package com.upc.bdm_hdfs.common;

import java.lang.Integer;
import java.lang.String;

public final class Constants {

	public static final Integer hdfs_port = 27000;
	public static final String hdfs_address = "hdfs://alakazam.fib.upc.es:" + hdfs_port;
	public static final String hdfs_config_dir = "/home/bdm/BDM_Software/hadoop/etc/hadoop/";
	public static final String user_path = "/user/bdm/";

	public static final String PLAIN = "plain";
	public static final String SEQUENCE = "sequence";
	public static final String AVRO = "avro";
	public static final String PARQUET = "parquet";

}
