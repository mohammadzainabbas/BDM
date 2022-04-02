package com.upc.bdm_hdfs.common.parser;

import java.lang.String;
import java.util.List;

public class Arguments {
    
    public boolean is_write = false;
    public String src_file_path = "";
    public boolean is_src_file_path_specified = false;
    public String desc_file_path = "";
    public boolean is_desc_file_path_specified = false;
    public String format = "";
    public boolean is_file_format_specified = false;
    public String all_args = "";

    public void parse(String[] args) {
        all_args = String.join(" ", args);
        for (int i = 0; (i < args.length) && (args[i] != null); i++) {
        //     all_args = all_args.concat(str).concat(" ");
            switch (args[i]) {
                case "-write":
                    is_write = true;       
                    break;

                case "-format":
                    is_file_format_specified = true;
                    format = args[i + 1];       
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
}
