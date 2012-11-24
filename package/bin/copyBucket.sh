#!/bin/bash

set -e
set -u

index="$1"
bucket="$2"
cold_path_destination="$3"

source java_executable.env
exec $JAVA -cp ./*:../lib/* com.splunk.shuttl.archiver.copy.CopiesBuckets "$index" "$bucket" "$cold_path_destination"
