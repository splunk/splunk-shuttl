#!/bin/sh

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

script_dir=$(dirname $0)
SPLUNK=$SPLUNK_HOME/bin/splunk

# Setup
# TODO: The search is made because we don't clean splunk.
#       The only code that belongs here is the splunk add oneshot wordfile-timestamp.
#       Because if the file has been added twice, the test should fail.
search=`$SPLUNK search 'source=*testdata-ts | head 1'`
if [ "$search" = "" ]; then
  $SPLUNK add oneshot $script_dir/testdata-ts
fi

sleep 15 

# Test

expected_splunk_out="\
ev
--
 5"

actual_splunk_out=$($SPLUNK search 'index="_internal" source="HadoopConnector" "group=per_source_thruput" series=source::*testdata-ts | table ev')

# Output
if [ "$expected_splunk_out" != "$actual_splunk_out" ]
then
  echo "Fail!
  Expected:
  \"$expected_splunk_out\"
  Actual:
  \"$actual_splunk_out\""
  exit 1
else
  exit 0
fi
