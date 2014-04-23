#!/bin/bash

set -e
set -u

SPLUNK_HOME="$1"
splunk="$SPLUNK_HOME"/bin/splunk
number_latest=0

while [ $number_latest != 6 ]; do
  cluster_status="`$splunk show cluster-bundle-status`"
  latest_bundle=`echo "$cluster_status" |
    head -n 4 | 
    tail -n 1 |
    cut -d = -f 2`
  number_latest=`echo "$cluster_status" | grep $latest_bundle 2> /dev/null | wc -l`
  sleep 2
done

