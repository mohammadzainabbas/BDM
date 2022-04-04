package com.upc.bdm_hdfs;

import java.lang.String;
import java.io.File;
import java.io.IOException;

import com.upc.bdm_hdfs.common.parser.Arguments;
import com.upc.bdm_hdfs.common.Constants;
import com.upc.bdm_hdfs.common.Utils;

import com.upc.bdm_hdfs.hdfs_writer.HDFSWriter;
import com.upc.bdm_hdfs.hdfs_writer.Parquet;

public class App 
{
    private static HDFSWriter writer;
    private static File schemaFile;

    public static void main( String[] args ) {
        Utils.print( "Welcome to HDFS parsing application!\n" );

        Arguments args_parser = new Arguments();
        args_parser.parse(args);

        Utils.print("args: " + args_parser.all_args);
        args_parser.check_sanity_checks();

        try {
            switch (args_parser.format) {
                case Constants.PLAIN:
                    Utils.print("Work in progress for: ".concat(args_parser.format));
                    break;
                case Constants.SEQUENCE:
                    Utils.print("Work in progress for: ".concat(args_parser.format));
                    break;
                case Constants.AVRO:
                    Utils.print("Work in progress for: ".concat(args_parser.format));
                    break;
                case Constants.PARQUET:
                    Utils.print("Writing as '" + args_parser.format + "' format.");
                    writer = new Parquet();
                    schemaFile = new File(args_parser.schema_file);
                    writer.write(args_parser.src_file_path, args_parser.desc_file_path, schemaFile);
                    break;
                default:
                    Utils.print(args_parser.format.concat(" is a non-supported HDFS file format."));
                    System.exit(1);
                    break;
                }
        } catch (IOException e) {
            Utils.print("[Error] IOException: Something went wrong");
        }





    }
}
