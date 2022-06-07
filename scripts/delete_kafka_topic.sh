#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 7th June, 2022
#====================================================================================
# This script is used to delete kafka topic on ubuntu. 
# useful link: https://stackoverflow.com/a/24288417/6390175
#====================================================================================

# Enable exit on error
set -e -u

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

#Function that shows usage for this script
function usage() {
cat << HEREDOC
Delete Kafka topic/stream

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -t, --topic             Name of the topic (aka stream) [by default 'random']
    -h, --help              Show usage

Examples:
 
    $ $progname -t bdm
    ⚐ → Deletes a kafka topic/stream with name 'bdm'.

HEREDOC
}

progname=$(basename $0)
KAFKA_DIR=~/BDM_Software/kafka_2.13-3.1.0
topic="random"

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -t|--topic) topic="$2"; shift ;;
        -h|--help)
        usage
        exit 1
        ;;
        *) printf "\n$progname: invalid option → '$1'\n\n⚐ Try '$progname -h' for more information\n\n"; exit 1 ;;
    esac
    shift
done

log "Deleting Kafka topic/stream '$topic'"

sh $KAFKA_DIR/bin/kafka-topics.sh --delete --topic $topic --bootstrap-server localhost:9092

log "Kafka topic/stream '$topic' deleted !!"

# bash scripts/delete_kafka_topic.sh -t bcn_events