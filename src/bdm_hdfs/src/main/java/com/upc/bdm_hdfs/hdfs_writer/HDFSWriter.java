package com.upc.bdm_hdfs.hdfs_writer;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;

public interface HDFSWriter {

    public void open(String filePath, File schemaFile) throws IOException;
	
	public void put(GenericRecord object);

	public void write(String src, String desc, File schemaFile) throws IOException;
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
}
