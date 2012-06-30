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

cd $SPLUNK_HOME/etc/apps/shuttl/

# the path to the bucket dir is passed in
bucket=$1

# derive the index name from the path
index=`dirname $1`
index=`dirname $index`
index=`basename $index`

exec -a splunk-bucket-freezer $JAVA_HOME/bin/java -cp ./bin/*:./lib/* com.splunk.shuttl.archiver.archive.BucketFreezer $index $bucket
