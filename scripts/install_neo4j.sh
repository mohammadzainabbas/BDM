#!/bin/bash
#====================================================================================
# Author: Mohammad Zain Abbas
# Date: 3rd June, 2022
#====================================================================================
# This script is used to install neo4j in ubuntu. 
# helping doc: https://neo4j.com/docs/operations-manual/current/installation/linux/debian/
# helping doc: https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-neo4j-on-ubuntu-20-04 (for remote access)
# helping doc: https://linuxhint.com/install-neo4j-ubuntu/
#====================================================================================

# Enable exit on error
set -e -u pipefail

# import helper functions from 'scripts/utils.sh'
. $(dirname $0)/utils.sh

#Function that shows usage for this script
usage() 
{
cat << HEREDOC

Install Neo4j in Ubuntu like distribution(s)

Usage: 
    
    $progname

Options:
    
    -h, --help                      Show usage

Examples:

    $ $progname
    ⚐ → Installs Neo4j

HEREDOC
}

#Get program name
progname=$(basename $0)

#Get all the arguments and update accordingly
while [[ "$#" -gt 0 ]]; do
    case $1 in
        -h|--help)
        usage
        exit 1
        ;;
        *) printf "\n$progname: invalid option → '$1'\n\n⚐ Try '$progname -h' for more information\n\n"; exit 1 ;;
    esac
    shift
done

start=$(date +%s)

log "Update apt repo"

sudo apt update

log "Add package(s) for HTTPS connections"

sudo apt install apt-transport-https ca-certificates curl software-properties-common

log "Add the GPG key to verify that the installation"

wget -O - https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -

log "Installating Neo4j"

echo 'deb https://debian.neo4j.com stable latest' | sudo tee -a /etc/apt/sources.list.d/neo4j.list
sudo apt-get update
sudo add-apt-repository universe
sudo apt-get install neo4j=1:4.4.7

end=$(date +%s)
time_took=$((end-start))

log "⚑ Installed Neo4j in $time_took seconds ..."

#====================================================================================
# To check the status of Neo4j service
# sudo systemctl status neo4j.service
#====================================================================================

#====================================================================================
# To stop Neo4j service
# sudo systemctl stop neo4j.service
#====================================================================================

#====================================================================================
# To start Neo4j service
# sudo systemctl start neo4j.service
#====================================================================================

#====================================================================================
# cypher-shell (to start cypher shell) [:exit to quit]
# username: neo4j
# password: zainzain
#====================================================================================
