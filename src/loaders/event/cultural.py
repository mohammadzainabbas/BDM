from os import remove
from os.path import join
from utils import get_hdfs_user_home, get_files, write_to_hdfs, print_log, csv_to_parquet

def main():
    activity_type = "culture"
    files, prefix = get_files(activity_type)
    print_log("Loading {} files of '{}' into HDFS".format(len(files), activity_type))
    hdfs_path = "{}/{}/{}".format(get_hdfs_user_home(), prefix, activity_type)
    for file in files:
        full_path, file_name = csv_to_parquet(file) # Convert to parquet and save it temp. at local
        print_log("File '{}' converted to parquet format and saved temporarily at '{}'".format(file, full_path))
        write_to_hdfs(join(hdfs_path, file_name), full_path) # Upload parquet file to HDFS
        print_log("File '{}' moved to HDFS at '{}'".format(full_path, hdfs_path))
        remove(full_path) # Remove parquet file from local
        print_log("Deleted parquet file '{}' from local".format(full_path))
    print_log("Saved {} file(s) of '{}' into HDFS at '{}'".format(len(files), activity_type, hdfs_path))

if __name__ == '__main__':
    main()