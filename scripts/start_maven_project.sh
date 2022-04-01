#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 1st April, 2022
#====================================================================================
# This script is used to create a quick maven project. 
# It will create a basic file structure for you to work with.
# Official doc: https://maven.apache.org/guides/mini/guide-creating-archetypes.html
#====================================================================================

# Enable exit on error
set -e -u -o pipefail

# import helper functions from 'scripts/utils.sh'
source $(dirname $0)/utils.sh

#Function that shows usage for this script
usage() 
{
cat << HEREDOC

Generate file structure for maven project

Usage: 
    
    $progname [OPTION] [Value]

Options:
    
    -gid, --group-id                Group/Package ID for your project. (by default "com.upc.bdm")
    -n, --name                      Name of your project. (by default "bdm")
    -v, --version                   Version of your project. (by default "1.0.0")
    -h, --help                      Show usage

Examples:

    $ $progname -gid com.upc.bdm -n bdm_p1
    ⚐ → Creates a "bdm_p1" project with package id as "com.upc.bdm".

HEREDOC
}

#Get program name
progname=$(basename $0)

group_id="com.upc.bdm"
name="bdm"
version="1.0.0"

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -gid|--group-id) group_id="$2"; shift ;;
        -n|--name) name="$2"; shift ;;
        -v|--version) version="$2"; shift ;;
        -h|--help)
        usage
        exit 1
        ;;
        *) printf "\n$progname: invalid option → '$1'\n\n⚐ Try '$progname -h' for more information\n\n"; exit 1 ;;
    esac
    shift
done

# some sanity checks
check_bin_2 mvn "--version"

log """Creating Java-Maven Project with:

name: $name
package-id: $group_id
version: $version

"""

start=$(date +%s)

mvn archetype:generate -DgroupId=$group_id -DartifactId=$name -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=$version -DinteractiveMode=false

end=$(date +%s)
time_took=$((end-start))

log "⚑ Boilerplate for project: $name was created in $time_took seconds ..."
