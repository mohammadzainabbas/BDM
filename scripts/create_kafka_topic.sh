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
function usage()
{
cat << HEREDOC
Create Kafka topic/stream
Usage: 
    
    $progname [OPTION] [Value]
Options:
    -t, --topic             Name of the topic (aka stream)
    -p, --path              Path for tpcds directory. (by default uses '../tpcds-kit')
    -h, --help              Show usage
Examples:
    $ $progname -s 1
    ⚐ → Benchmark for 1 Gb scale.
    $ $progname -s 5 -p ../tpcds-kit/
    ⚐ → Benchmark for 5 Gb scale with tpcds dir path as '../tpcds-kit'.
HEREDOC
}


KAFKA_DIR=~/BDM_Software/kafka_2.13-3.1.0
topic=$1

log "Creating Kafka topic"

sh $KAFKA_DIR/bin/kafka-topics.sh --create --topic $topic --bootstrap-server localhost:9092

log "Kafka topic '$topic' started !!"
