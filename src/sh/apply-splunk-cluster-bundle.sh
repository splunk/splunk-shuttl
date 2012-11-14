#!/bin/bash

set -e
set -u

script_dir=$(dirname $0)
SPLUNK_HOME=$1
user=$2
pass=$3

splunk=$SPLUNK_HOME/bin/splunk

$splunk apply cluster-bundle --answer-yes -auth $user:$pass
echo "Waiting for the new cluster bundle to be applied to all slaves..."
sleep 5
$script_dir/wait-for-new-cluster-bundle.sh $SPLUNK_HOME

