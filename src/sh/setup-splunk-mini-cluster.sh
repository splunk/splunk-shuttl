#!/bin/bash

set -e
set -u

# Set splunk cluster variables.
MASTER_NAME=$1
MASTER_PORT=$2
SLAVE1_NAME=$3
SLAVE1_PORT=$4
SLAVE2_NAME=$5
SLAVE2_PORT=$6

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`
source $SHUTTL_HOME/src/sh/ant-call-build-xml.sh

setup-splunk-instance() {
  server_name=$1
  mgmt_port=$2
  call_ant_with_string_args "\
      -Dsplunk.mgmtport=$mgmt_port \
      -Dsplunk.server.name=$server_name \
      splunk-setup"
}

setup-splunk-instance $MASTER_NAME $MASTER_PORT 
setup-splunk-instance $SLAVE1_NAME $SLAVE1_PORT
setup-splunk-instance $SLAVE2_NAME $SLAVE2_PORT

make-cluster-master() {
  server_name=$1
  mgmt_port=$2
  call_ant_with_string_args "\
      -Dsplunk.mgmtport=$mgmt_port \
      -Dsplunk.server.name=$server_name \
      splunk-make-cluster-master-with-replication-2"
}

make-cluster-slave() {
  server_name=$1
  mgmt_port=$2
  replication_port=$3
  call_ant_with_string_args "\
      -Dsplunk.mgmtport=$mgmt_port \
      -Dsplunk.server.name=$server_name \
      -Dcluster.replication.port=$replication_port \
      -Dcluster.master.uri=https://localhost:$MASTER_PORT \
      splunk-make-slave"
}

make-cluster-master $MASTER_NAME $MASTER_PORT
make-cluster-slave $SLAVE1_NAME $SLAVE1_PORT 8214
make-cluster-slave $SLAVE2_NAME $SLAVE2_PORT 8215

