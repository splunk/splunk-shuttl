#!/bin/bash

# Script that will down load a 1 MB file that can be indexed and rolled by Splunk.
# The script will loop forever, hence it's a manual test.

set -e
set -u

script_dir=$(dirname $0)
shuttl_home=`$script_dir/../print-shuttl-home.sh`
splunk_home=$shuttl_home/build-cache/splunk-1

$script_dir/echo_1mb_text.sh > $splunk_home/loremIpsum.txt

while true
do
  $splunk_home/bin/splunk \
      add oneshot $splunk_home/loremIpsum.txt \
      -index archiver-test-index \
      -auth admin:changed
  sleep 5
done
