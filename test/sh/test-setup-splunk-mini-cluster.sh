#!/bin/bash

set -e
set -u

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`
source $SHUTTL_HOME/src/sh/ant-call-build-xml.sh

master_name=splunk-master
master_port=8070
slave1_name=splunk-slave1
slave1_port=8071
slave2_name=splunk-slave2
slave2_port=8072

teardown_splunk() {
  name=$1
  port=$2
  call_ant_with_string_args "\
      -Dsplunk.server.name=$name \
      -Dsplunk.mgmtport=$port \
      splunk-teardown"
}

teardown() {
  teardown_splunk $master_name $master_port
  teardown_splunk $slave1_name $slave1_port
  teardown_splunk $slave2_name $slave2_port
}

run_script() {
  $SHUTTL_HOME/src/sh/setup-splunk-mini-cluster.sh \
      $master_name \
      $master_port \
      $slave1_name \
      $slave1_port \
      $slave2_name \
      $slave2_port
}

run_script
teardown

