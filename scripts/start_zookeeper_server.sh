#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 7th June, 2022
#====================================================================================
# This script is used to start Zookeeper server on ubuntu. 
#====================================================================================

# Enable exit on error
set -e -u pipefail

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

KAFKA_DIR=~/BDM_Software/kafka_2.13-3.1.0

log "Starting Zookeeper Server"

sh $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties

log "Zookeeper server started !!"
