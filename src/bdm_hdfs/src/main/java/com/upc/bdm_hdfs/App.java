package com.upc.bdm_hdfs;

import java.lang.String;
import com.upc.bdm_hdfs.common.parser.Arguments;

public class App 
{
    protected static void print(String text) {
        System.out.println( text );
    }
    public static void main( String[] args ) {
        print( "Welcome to HDFS parsing application!\n" );

        Arguments args_parser = new Arguments();
        args_parser.parse(args);

        print("args: " + args_parser.all_args);
        print("is_write: " + args_parser.is_write);

        print("is_format: " + args_parser.is_file_format_specified);
        print("format: " + args_parser.format);
        
        print("is_src: " + args_parser.is_src_file_path_specified);
        print("src: " + args_parser.src_file_path);

        print("is_desc: " + args_parser.is_desc_file_path_specified);
        print("desc: " + args_parser.desc_file_path);
        
    }
}
