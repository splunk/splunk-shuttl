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

MASTER_SHUTTL_PORT=$7
SLAVE1_SHUTTL_PORT=$8
SLAVE2_SHUTTL_PORT=$9

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`
source $SHUTTL_HOME/src/sh/ant-call-build-xml.sh

setup-splunk-instance() {
  server_name=$1
  mgmt_port=$2
  shuttl_port=$3
  call_ant_with_string_args "\
      -Dsplunk.mgmtport=$mgmt_port \
      -Dsplunk.server.name=$server_name \
      -Dshuttl.port=$shuttl_port \
      splunk-setup"
}

setup-splunk-instance $MASTER_NAME $MASTER_PORT $MASTER_SHUTTL_PORT
setup-splunk-instance $SLAVE1_NAME $SLAVE1_PORT $SLAVE1_SHUTTL_PORT
setup-splunk-instance $SLAVE2_NAME $SLAVE2_PORT $SLAVE2_SHUTTL_PORT

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
      -Dcluster.master.port=$MASTER_PORT \
      splunk-make-slave"
}

make-cluster-master $MASTER_NAME $MASTER_PORT
make-cluster-slave $SLAVE1_NAME $SLAVE1_PORT 8214
make-cluster-slave $SLAVE2_NAME $SLAVE2_PORT 8215

create-fast-rolling-cluster-index() {
  server_name=$1
  call_ant_with_string_args "\
      -Dsplunk.server.name=$server_name \
      splunk-cluster-fast-rolling-index"
}

create-fast-rolling-cluster-index $MASTER_NAME 

apply-cluster-bundle() {
  server_name=$1
  call_ant_with_string_args "\
      -Dsplunk.server.name=$server_name \
      splunk-cluster-bundle-apply"
}

apply-cluster-bundle $MASTER_NAME


