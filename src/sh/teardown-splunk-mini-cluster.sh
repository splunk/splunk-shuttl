#!/bin/bash

set -e
set -u

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`
source $SHUTTL_HOME/src/sh/ant-call-build-xml.sh

master_name=$1
master_port=$2
slave1_name=$3
slave1_port=$4
slave2_name=$5
slave2_port=$6

teardown_splunk() {
  name=$1
  port=$2
  call_ant_with_string_args "\
      -Dsplunk.server.name=$name \
      -Dsplunk.mgmtport=$port \
      splunk-teardown"
}

teardown_cluster() {
  teardown_splunk $master_name $master_port
  teardown_splunk $slave1_name $slave1_port
  teardown_splunk $slave2_name $slave2_port
}

teardown_cluster

