package com.upc.bdm_hdfs;

import java.lang.String;
import com.upc.bdm_hdfs.common.parser.Arguments;
import com.upc.bdm_hdfs.hdfs_writer.HDFSWriter;

public class App 
{
    protected static void print(String text) {
        System.out.println( text );
    }

    private static HDFSWriter writer;

    /** Temp. usage */
    private static void print_args(Arguments args_parser) {
        print("is_write: " + args_parser.is_write);
        print("is_format: " + args_parser.is_file_format_specified);
        print("format: " + args_parser.format);
        print("is_src: " + args_parser.is_src_file_path_specified);
        print("src: " + args_parser.src_file_path);
        print("is_desc: " + args_parser.is_desc_file_path_specified);
        print("desc: " + args_parser.desc_file_path);
    }

    private static void check_sanity_checks(Arguments args_parser) {
        if (!args_parser.is_write || !args_parser.is_desc_file_path_specified || !args_parser.is_file_format_specified || !args_parser.is_src_file_path_specified || args_parser.format.length() == 0 || args_parser.src_file_path.length() == 0 || args_parser.desc_file_path.length() == 0) {
            print("\n");
            print_args(args_parser);
            print("\nPlease provide all required arguments!\n");
            System.exit(1);
        }
    }
    public static void main( String[] args ) {
        print( "Welcome to HDFS parsing application!\n" );

        Arguments args_parser = new Arguments();
        args_parser.parse(args);

        print("args: " + args_parser.all_args);
        check_sanity_checks(args_parser);

        switch (args_parser.format) {
            case "plain":
                print("Work in progress for: ".concat(args_parser.format));
                break;
            case "sequence":
                print("Work in progress for: ".concat(args_parser.format));
                break;
            case "avro":
                print("Work in progress for: ".concat(args_parser.format));
                break;
            case "parquet":
                print("Work in progress for: ".concat(args_parser.format));
                break;
            default:
                print(args_parser.format.concat(" is a non-supported HDFS file format."));
                System.exit(1);
                break;
        }




    }
}
