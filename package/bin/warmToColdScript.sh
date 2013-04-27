#!/bin/bash

# warmToColdScript.sh - the Shuttl copy script to be called by Splunk
#
# Example configuration (indexes.conf)
#
#  [archiver-test-index]
#  homePath   = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/db
#  coldPath   = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/colddb
#  thawedPath = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/thaweddb
#  warmToColdScript = $SPLUNK_HOME/etc/apps/shep/bin/warmToColdScript.sh
#
#
# Copyright (C) 2012 Splunk Inc.
#
# Splunk Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
set -u

cd $SPLUNK_HOME/etc/apps/shuttl/bin

bucket="$1"
cold_path_destination="$2"

mv $bucket $cold_path_destination

source java_executable.env
$JAVA -cp ./*:../lib/* com.splunk.shuttl.archiver.copy.ColdCopyEntryPoint "$cold_path_destination" &

# The sleep below is for when testing the script through Java.
# For some reason, the ColdCopyEntryPoint won't have time to start,
# before the script ends. And it seems like all background processes
# are killed/slayed when Java ends it's shell process.
if [ "$#" -gt 2 ]; then
  sleep 3 # Java sleep.
fi
