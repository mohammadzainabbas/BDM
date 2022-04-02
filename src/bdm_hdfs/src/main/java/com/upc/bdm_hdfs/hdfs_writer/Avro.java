//package com.upc.bdm_hdfs.hdfs_writer;
//
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//
//import org.apache.avro.file.DataFileWriter;
//import org.apache.avro.io.DatumWriter;
//import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import bdm.labs.hdfs.Constants;
//
//public class MyAvroFileWriter implements MyWriter {
//
//	private Configuration config;
//	private FileSystem fs;
//
//
//	DataFileWriter<Adult> dataFileWriter;
//
//	public void print(String text) {
//		System.out.println(text);
//	}
//
//	public MyAvroFileWriter() throws IOException {
//		this.config = new Configuration();
//		dataFileWriter = null;
//		this.reset();
//	}
//
//	public void open(String file) throws IOException {
//		this.config = new Configuration();
//		config.set("fs.hdfs.impl",
//		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//		    );
//		config.set("fs.file.impl",
//		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
//		    );
//		try {
//			this.fs = FileSystem.get(new URI(Constants.hdfs_address), config);
//		}
//		catch (URISyntaxException e) {
//			e.printStackTrace();
//		}
//		Path path = new Path(userPath + file);
//		if (this.fs.exists(path)) {
//			System.out.println("File " + file + " already exists!");
//			System.exit(1);
//		}
//
//		DatumWriter<Adult> wineInfoDatumWriter = new SpecificDatumWriter<Adult>(Adult.class);
//		dataFileWriter = new DataFileWriter<Adult>(wineInfoDatumWriter);
//		dataFileWriter.create(Adult.getClassSchema(), this.fs.create(path));
//	}
//
//	public void put(Adult w) {
//		try {
//			this.dataFileWriter.append(w);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//
//	public void reset() {
//
//	}
//
//
//	public int flush() throws IOException {
//		this.dataFileWriter.flush();
//		return 1;
//	}
//
//	public void close() throws IOException {
//		this.dataFileWriter.close();
//	}
//
//}
