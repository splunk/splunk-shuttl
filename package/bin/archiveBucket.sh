#!/usr/bin/env sh

# start.sh
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

cd $SPLUNK_HOME/etc/apps/shep/

index=$1
bucket=$2

exec -a BucketFrezer $JAVA_HOME/bin/java -cp ./bin/*:./lib/* com.splunk.shep.archiver.archive.BucketFreezer $index $bucket
