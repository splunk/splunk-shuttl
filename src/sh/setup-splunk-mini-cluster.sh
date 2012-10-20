#!/bin/bash

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`

setup-splunk-instance() {
  mgmt_port=$1
  server_name=$2
  ant -f $SHUTTL_HOME/build.xml \
      -Dsplunk.mgmtport=$mgmt_port \
      -Dsplunk.server.name=$server_name \
      splunk-setup
}


setup-splunk-instance 8081 splunk-1
setup-splunk-instance 8082 splunk-2
setup-splunk-instance 8083 splunk-3


