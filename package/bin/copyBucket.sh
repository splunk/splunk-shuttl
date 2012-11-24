#!/bin/bash

set -e
set -u

cd $SPLUNK_HOME/etc/apps/shuttl/bin

bucket="$2"
cold_path_destination="$3"

source java_executable.env
exec $JAVA -cp ./*:../lib/* com.splunk.shuttl.archiver.copy.CopiesBuckets "$bucket" "$cold_path_destination"
