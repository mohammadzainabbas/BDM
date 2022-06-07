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

KAFKA_DIR=~/BDM_Software/kafka_2.13-3.1.0
topic=$1

log "Creating Kafka topic"

sh $KAFKA_DIR/bin/kafka-topics.sh --create --topic $topic --bootstrap-server localhost:9092

log "Kafka topic '$topic' started !!"
