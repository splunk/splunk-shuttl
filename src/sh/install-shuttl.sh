#!/bin/bash

# Usage: Either set $SPLUNK_HOME or pass the path to splunk in $1 (first argument)
# Does: Clones and builds Shuttl. Then extracts the built Shuttl to specified
#       splunk/etc/apps/shuttl

set -e
set -u
script_dir=$(dirname $0)

if [ "$#" -eq 0 ]; then
    splunk_home="$SPLUNK_HOME"
else
    splunk_home="$1"
fi

git clone https://github.com/splunk/splunk-shuttl.git

shuttl_clone_home="$script_dir"/splunk-shuttl

(cd $shuttl_clone_home && ./buildit.sh)
shuttl_spl=$shuttl_clone_home/build/shuttl.spl

tar -xf $shuttl_spl -C $splunk_home/etc/apps

