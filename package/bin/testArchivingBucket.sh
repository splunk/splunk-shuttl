#!/bin/bash

# Script for testing Shuttl configuration.
# The script archives a fake bucket, and you
# can look at the Shuttl logs 
# ($SPLUNK_HOME/var/log/splunk/shuttl.log)
# to see if everything works, instead of
# making a bucket "roll" to cold or frozen.

# Usage: ./testArchivingBucket.sh <index> [host] [port] 
#        Configure a splunk index and pass
#        the name of the index to this script.
#        If you've configured the Shuttl host and
#        port to be something else than the 
#        default, then you need to pass them as
#        second and third argument.

# Note: You want to delete this bucket after it's
#       been archived/shuttl'ed.

set -u

default_shuttl_host=@SHUTTL.HOST@
default_shuttl_port=@SHUTTL.PORT@

print_usage() {
    echo 1>&2 "$0 [shuttl_port] [shuttl_host]"
}

if [ $# -lt 1 ]; then
    shuttl_port=$default_shuttl_port
    shuttl_host=$default_shuttl_host
elif [ $# -eq 1 ]; then
   shuttl_port=$1
   shuttl_host=$default_shuttl_host
elif [ $# -eq 2 ]; then
   shuttl_port=$1
   shuttl_host=$2
else
    echo 1>&2 "Usage:"
    print_usage
    exit 1
fi

bucket_dir=$(mktemp -d -t db_1336330530_1336330530_0)
mkdir $bucket_dir/rawdata
touch $bucket_dir/rawdata/journal.gz
touch $bucket_dir/rawdata/slizes.dat

splunk_index="TestIndexForTryingOutShuttlArchiving"

echo 1>&2 "If you're having trouble, you can customize the shuttl vars with usage:"
print_usage
echo 1>&2 

echo 1>&2 "Shuttling bucket with:
    index=$splunk_index
    shuttl_host=$shuttl_host
    shuttl_port=$shuttl_port"


curl -sS -X POST \
    -d "path=$bucket_dir&index=$splunk_index" \
    http://$shuttl_host:$shuttl_port/shuttl/rest/archiver/bucket/archive
curl_exit=$?
echo 1>&2

if [ $curl_exit -ne 0 ]; then
    echo 1>&2 "Failure!"
    echo 1>&2
fi

echo 1>&2 "Remember to delete all the archived buckets under $splunk_index
from the configured backend storage, when you're done testing."
