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

hadoop=$HADOOP_HOME/bin/hadoop
testFolder=/hadoopunittest1

# Setup
$hadoop fs -put file01 "$testFolder/input/file01" &>/dev/null
$hadoop fs -put file02 "$testFolder/input/file02" &>/dev/null

# Test
$hadoop jar $SPLBRANCH/build/jar/splunk_hadoop_unittests.jar com.splunk.mapreduce.lib.rest.tests.WordCount "$testFolder/input" "$testFolder/output$1"

expected_splunk_out="\
FIELDNAME
---------
Bye 1
Goodbye 1
Hadoop 2
Hello 2
World 2"

actual_splunk_out=$(splunk search 'index=main sourcetype="hadoop_event" | rex "(?i)^(?:[^ ]* ){6}(?P<FIELDNAME>.+)" | table FIELDNAME | tail 5')

# Tear down
$hadoop fs -rmr "$testFolder" &>/dev/null
# TODO cleanup splunk

# Output
if [ "$expected_splunk_out" != "$actual_splunk_out" ]
then
  echo "Fail!
Expected:
\"$expected_splunk_out\"
Actual:
\n\"$actual_splunk_out\""
  exit 1
else
  exit 0
fi
