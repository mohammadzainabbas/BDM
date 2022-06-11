#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 11th June, 2022
#====================================================================================
# This script is used to read kafka topic/stream on ubuntu. 
# doc: https://kafka.apache.org/quickstart
#====================================================================================

# Enable exit on error
set -e -u

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

#Function that shows usage for this script
function usage() {
cat << HEREDOC
Read Kafka topic/stream

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -t, --topic             Name of the topic (aka stream) [by default 'random']
    -h, --help              Show usage

Examples:
 
    $ $progname -t bdm
    ⚐ → Reads a kafka topic/stream with name 'bdm'.

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

log "Reading Kafka topic/stream from the beginning"

sh $KAFKA_DIR/bin/kafka-console-consumer.sh --topic $topic --from-beginning --bootstrap-server localhost:9092

log "Read Kafka topic/stream '$topic' from beginning !!"

# bash scripts/read_kafka_stream.sh -t bcn_events