#!/usr/bin/env /bin/bash

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

$hadoop dfs -put file01 /wordcount/input/file01
$hadoop dfs -put file02 /wordcount/input/file02

$hadoop jar $SPLBRANCH/build/jar/splunk_hadoop_unittests.jar com.splunk.mapreduce.lib.rest.tests.WordCount /wordcount/input /wordcount/output$1

expected_splunk_out="Sun+Nov+27+22%3A34%3A03+PST+2011+World+2
Sun+Nov+27+22%3A34%3A03+PST+2011+Hello+2
Sun+Nov+27+22%3A34%3A03+PST+2011+Hadoop+2
Sun+Nov+27+22%3A34%3A03+PST+2011+Goodbye+1
Sun+Nov+27+22%3A34%3A03+PST+2011+Bye+1"
actual_splunk_out=$(splunk search 'source=wordcount sourcetype=hadoop_event' | tail -5)
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
