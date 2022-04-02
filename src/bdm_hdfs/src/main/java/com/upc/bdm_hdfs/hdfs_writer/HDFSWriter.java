package com.upc.bdm_hdfs.hdfs_writer;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

public interface HDFSWriter {
    public void open(String file, File schemaFile) throws IOException;
	
	public void put(GenericRecord obj);
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
}
