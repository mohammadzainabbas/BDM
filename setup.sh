#! /bin/bash

set -e -u

sudo apt update && apt upgrade -y
sudo apt install git gcc make zlib1g-dev python3-pip -y

sudo apt-get install bison flex gdb -y

cd 

git clone https://github.com/postgres/postgres.git

cd postgres

# git checkout REL_13_STABLE

./configure --without-readline --enable-debug
# ./configure --without-readline --enable-cassert --enable-debug CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"

make && make install

mkdir /usr/local/pgsql/data

echo 'export PATH="/usr/local/pgsql/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc

useradd -m postgres

chown postgres /usr/local/pgsql/data

su - postgres -c bash

# From Postgres Documentation

# ./configure
# make
# su
# make install
# adduser postgres
# mkdir /usr/local/pgsql/data
# chown postgres /usr/local/pgsql/data
# su - postgres
# echo 'export PATH="/usr/local/pgsql/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
# initdb -D /usr/local/pgsql/data
# pg_ctl -D /usr/local/pgsql/data -l logfile start
# createdb test
# psql test

