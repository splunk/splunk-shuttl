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
HADOOP=$HADOOP_HOME/bin/hadoop
SPLUNK=$SPLUNK_HOME/bin/splunk
testFolder=/hadoopunittest2

# Setup
# TODO: The search is made because we don't clean splunk.
#       The only code that belongs here is the splunk add oneshot wordfile-timestamp.
#       Because if the file has been added twice, the test should fail.
search=`splunk search 'source=*wordfile-timestamp | head 1'`
if [ "$search" = "" ]; then
  $SPLUNK add oneshot $script_dir/wordfile-timestamp
fi

# Test
$HADOOP jar $SPLBRANCH/build/jar/splunk_hadoop_unittests.jar com.splunk.shep.mapreduce.lib.rest.tests.WordCount2 "$testFolder/input" "$testFolder/output$1"

expected_splunk_out="\
2011-09-19	300
a	300
is	300
test	300
this	300"

actual_splunk_out=$($HADOOP_HOME/bin/hadoop dfs -cat "$testFolder/output$1/part-00000" | tail -n 5)

# Teardown
$HADOOP fs -rmr "$testFolder" &>/dev/null
# TODO clean splunk

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
