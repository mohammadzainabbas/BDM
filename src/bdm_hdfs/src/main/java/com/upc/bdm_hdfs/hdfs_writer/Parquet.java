package com.upc.bdm_hdfs.hdfs_writer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class Parquet implements HDFSWriter {

    public void open(String file) throws IOException;
	
	public void put(Object obj);
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
}
