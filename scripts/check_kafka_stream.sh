#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 7th June, 2022
#====================================================================================
# This script is used to list all kafka topic/streams on ubuntu. 
#====================================================================================

# Enable exit on error
set -e -u

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

KAFKA_DIR=~/BDM_Software/kafka_2.13-3.1.0

log "Listing Kafka topic/stream"

sh $KAFKA_DIR/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

sh $KAFKA_DIR/bin/kafka-console-consumer.sh --topic activities --from-beginning --bootstrap-server localhost:9092

log "All done !!"
