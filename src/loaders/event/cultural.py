from os.path import join
from utils import get_hdfs_user_home, get_files, write_to_hdfs, print_log

def main():
    activity_type = "culture"
    files, prefix = get_files(activity_type)
    print_log("Loading {} files of '{}' into HDFS".format(len(files), activity_type))
    hdfs_path = "{}/{}/{}".format(get_hdfs_user_home(), prefix, activity_type)
    for file in files:
        file_name = str(file).split("/")[-1]
        print_log("File '{}' moved to HDFS at '{}'".format(file, hdfs_path))
        write_to_hdfs(join(hdfs_path, file_name), file)
    print_log("Saved {} file(s) of '{}' into HDFS at '{}'".format(len(files), activity_type, hdfs_path))

if __name__ == '__main__':
    main()