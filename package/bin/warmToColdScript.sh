#!/bin/bash

set -e
set -u

cd $SPLUNK_HOME/etc/apps/shuttl/bin

bucket="$1"
cold_path_destination="$2"

mv $bucket $cold_path_destination

source java_executable.env
exec $JAVA -cp ./*:../lib/* com.splunk.shuttl.archiver.copy.ColdCopyEntryPoint "$cold_path_destination"
