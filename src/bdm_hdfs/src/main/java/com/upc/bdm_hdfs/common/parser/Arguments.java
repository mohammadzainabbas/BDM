package com.upc.bdm_hdfs.common.parser;

import java.lang.String;
import com.upc.bdm_hdfs.common.Utils;
import com.upc.bdm_hdfs.common.Constants;

public class Arguments {
    
    public boolean is_write = false;
    public String src_file_path = "";
    public boolean is_src_file_path_specified = false;
    public String desc_file_path = "";
    public boolean is_desc_file_path_specified = false;
    public String format = "";
    public boolean is_file_format_specified = false;
    public String schema_file = "";
    public boolean is_schema_file_format_specified = false;
    public String all_args = "";

    public void parse(String[] args) {
        all_args = String.join(" ", args);
        for (int i = 0; (i < args.length) && (args[i] != null); i++) {
            switch (args[i]) {
                case "-write":
                    is_write = true;       
                    break;

                case "-format":
                    is_file_format_specified = true;
                    format = args[i + 1];       
                    break;

                case "-schema":
                    is_schema_file_format_specified = true;
                    schema_file = args[i + 1];       
                    break;

                case "-src":
                    is_src_file_path_specified = true;
                    src_file_path = args[i + 1];       
                    break;

                case "-desc":
                    is_desc_file_path_specified = true;
                    desc_file_path = args[i + 1];       
                    break;
            
                default:
                    break;
            }
        }
    }

    /** Temp. usage */
    public void print_args() {
        Utils.print("is_write: " + is_write);
        Utils.print("is_format: " + is_file_format_specified);
        Utils.print("format: " + format);
        Utils.print("is_src: " + is_src_file_path_specified);
        if (format == Constants.AVRO || format == Constants.PARQUET) {
            Utils.print("schema_file: " + schema_file);
            Utils.print("is_schema: " + is_schema_file_format_specified);
        }
        Utils.print("src: " + src_file_path);
        Utils.print("is_desc: " + is_desc_file_path_specified);
        Utils.print("desc: " + desc_file_path);
    }
    
    private void print_error() {
        Utils.print("\n");
        print_args();
        Utils.print("\nPlease provide all required arguments!\n");
        System.exit(1);
    }
    
    public void check_sanity_checks() {
        if (!is_write || !is_desc_file_path_specified || !is_file_format_specified || !is_src_file_path_specified || format.length() == 0 || src_file_path.length() == 0 || desc_file_path.length() == 0) {
            print_error();
        }
        if (format.equals(Constants.AVRO) || format.equals(Constants.PARQUET)) {
            if (!is_schema_file_format_specified || schema_file.length() == 0) {
                print_error();
            }    
        }
    }
}
