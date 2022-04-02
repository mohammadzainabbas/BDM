package com.upc.bdm_hdfs.hdfs_writer;

import java.io.IOException;

public interface HDFSWriter {
    public void open(String file) throws IOException;
	
	public void put(Object obj);
	
	public void reset();
	
	public int flush() throws IOException;
	
	public void close() throws IOException;
}
