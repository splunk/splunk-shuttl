#!/bin/bash

# coldToFrozenScript.sh - the Shuttl archive script to be called by Splunk
#
# Example configuration (indexes.conf)
#
#  [archiver-test-index]
#  homePath   = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/db
#  coldPath   = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/colddb
#  thawedPath = $SPLUNK_HOME/var/lib/splunk/archiver-test-index/thaweddb
#  coldToFrozenScript = $SPLUNK_HOME/etc/apps/shep/bin/coldToFrozenScript.sh
#
#
# Copyright (C) 2011 Splunk Inc.
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


: ${SPLUNK_HOME:?"Need to set SPLUNK_HOME to non-empty"}

cd $SPLUNK_HOME/etc/apps/shuttl/bin

if [ $# -lt 1 ]; then
    echo 1>&2 "usage: $0 <bucket>"
    exit 1
fi

bucket=$1

source java_executable.env
exec $JAVA -cp ./*:../lib/* com.splunk.shuttl.archiver.archive.BucketFreezer $bucket
