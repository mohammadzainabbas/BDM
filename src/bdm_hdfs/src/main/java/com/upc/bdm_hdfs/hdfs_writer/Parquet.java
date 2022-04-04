package com.upc.bdm_hdfs.hdfs_writer;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.upc.bdm_hdfs.common.Constants;
import com.upc.bdm_hdfs.common.Utils;

public class Parquet implements HDFSWriter {

	private Configuration configuration;
	private FileSystem fileSystem;
	private Schema schema;
	private AvroParquetWriter<Object> avroParquetWriter;

	public Parquet() throws IOException {
		this.configuration = new Configuration();
		this.avroParquetWriter = null;
		this.reset();
	}

	public void open(String filePath, File schemaFile) throws IOException {
		if (schemaFile != null) {
			this.schema = new Schema.Parser().parse(schemaFile);
		}

		this.configuration = new Configuration();
		this.configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName() );
		this.configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
		
		try {
			this.fileSystem = FileSystem.get(new URI(Constants.hdfs_address), this.configuration);
		}
		catch (URISyntaxException e) {
			e.printStackTrace();
		}

		Path path = new Path(Constants.hdfs_address + filePath);

		if (this.fileSystem.exists(path)) {
			System.out.println("File " + filePath + " already exists!");
			System.exit(1);
		}
		this.avroParquetWriter = new AvroParquetWriter<Object>(path, this.schema,
				CompressionCodecName.UNCOMPRESSED,
				ParquetWriter.DEFAULT_BLOCK_SIZE,
				ParquetWriter.DEFAULT_PAGE_SIZE,
				true,
				this.configuration);
		/*try {
			parquetWriter = (AvroParquetWriter<Object>) AvroParquetWriter.builder(new Path(new URI("hdfs://10.4.41.154:27000/"+file))).build();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}

	public void put(Object object) {
		try {
			this.avroParquetWriter.write(object);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void write(String src, String desc, File schemaFile) throws IOException {
		try {
			
			open(desc, schemaFile);
			
			File srcFile = new File(src);
			if (!srcFile.exists()) { 
				Utils.print("[Error]: File not found");
				System.exit(1);
			}
			
			for(String line: FileUtils.readLines(srcFile) {
				put(line);
			}
			
			close();
		
		} catch (IOException e) {
			//TODO: handle exception
		}

	}
	
	public void reset() {	
	}
	
	public int flush() throws IOException {
		return 1;
	}
	
	public void close() throws IOException {
		this.avroParquetWriter.close();
	}
}
