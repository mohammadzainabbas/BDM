#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 7th June, 2022
#====================================================================================
# This script is used to create kafka topic on ubuntu. 
#====================================================================================

# Enable exit on error
set -e -u pipefail

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

#Function that shows usage for this script
usage() {
cat << HEREDOC
Create Kafka topic/stream

Usage: 
    
    $progname [OPTION] [Value]

Options:

    -t, --topic             Name of the topic (aka stream) [by default 'random']
    -h, --help              Show usage

Examples:
 
    $ $progname -t bdm
    ⚐ → Creates a kafka topic/stream with name 'bdm'.

HEREDOC
}

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

log "Creating Kafka topic/stream"

# sh $KAFKA_DIR/bin/kafka-topics.sh --create --topic $topic --bootstrap-server localhost:9092

log "Kafka topic/stream '$topic' started !!"
