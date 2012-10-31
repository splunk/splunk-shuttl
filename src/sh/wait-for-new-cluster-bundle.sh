#!/bin/bash

set -e
set -u

SPLUNK_HOME="$1"
splunk="$SPLUNK_HOME"/bin/splunk
number_latest=0

while [ $number_latest != 4 ]; do
  cluster_status="`$splunk show cluster-bundle-status`"
  latest_bundle=`echo "$cluster_status" | grep "Latest" | head -n 1 | awk '{print $3}'`
  number_latest=`echo "$cluster_status" | grep $latest_bundle | wc -l`
  sleep 1
done

