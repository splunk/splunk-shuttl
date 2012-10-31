#!/bin/bash

set -e
set -u

script_dir=$(dirname $0)
SPLUNK_HOME=$1
splunk=$SPLUNK_HOME/bin/splunk

$splunk apply cluster-bundle --answer-yes
sleep 5
$script_dir/wait-for-new-cluster-bundle.sh $SPLUNK_HOME

