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
testFolder=/eventsunittest1

# Setup
$HADOOP fs -mkdir "$testFolder"
$HADOOP fs -put "$script_dir/splunkdatatest1" "$testFolder/splunkdatatest1"
$HADOOP fs -put "$script_dir/splunkdatatest2" "$testFolder/splunkdatatest2"

# Test
$HADOOP jar $SHEPDIR/build/jar/splunk_hadoop_unittests.jar com.splunk.shep.mapreduce.lib.rest.tests.SplunkEventReader "$testFolder/splunkdata*" "$testFolder/output$1"

$HADOOP fs -get "$testFolder/output$1/_SUCCESS"  _SUCCESS

# Check run
if [ -e _SUCCESS ]
then
  retval=0
else
  echo "Fail - SplunkEventsInputFormat not working"
  retval=1
fi

# Teardown
$HADOOP fs -rmr "$testFolder" &>/dev/null
rm -f _SUCCESS

exit $retval
