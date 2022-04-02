package com.upc.bdm_hdfs;

import java.lang.String;
import com.upc.bdm_hdfs.common.parser.Arguments;
import com.upc.bdm_hdfs.hdfs_writer.HDFSWriter;
import com.upc.bdm_hdfs.common.Constants;
import com.upc.bdm_hdfs.common.Utils;

public class App 
{
    private static HDFSWriter writer;

    public static void main( String[] args ) {
        Utils.print( "Welcome to HDFS parsing application!\n" );

        Arguments args_parser = new Arguments();
        args_parser.parse(args);

        Utils.print("args: " + args_parser.all_args);
        args_parser.check_sanity_checks();

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
                Utils.print("Work in progress for: ".concat(args_parser.format));
                break;
            default:
                Utils.print(args_parser.format.concat(" is a non-supported HDFS file format."));
                System.exit(1);
                break;
        }




    }
}
