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
export CLASSPATH=../../../build/jar/splunk_hadoop_unittests.jar
export CLASSPATH=$CLASSPATH:../../../contrib/wikixmlj/bzip2.jar
export CLASSPATH=$CLASSPATH:../../../contrib/wikixmlj/xercesImpl.jar
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/conf
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/hadoop-core-0.20.203.0.jar
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/commons-logging-1.1.1.jar
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/commons-configuration-1.6.jar
export CLASSPATH=$CLASSPATH:$HADOOP_HOME/lib/commons-lang-2.4.jar

if [ $# = 0 ]; then
  java com.splunk.mapreduce.lib.rest.tests.util.WikipediaHDFSLoader http://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles1.xml-p000000010p000010000.bz2 hdfs://localhost:9000/wordcount/wikitest/wiki2.seq
else
  java com.splunk.mapreduce.lib.rest.tests.util.WikipediaHDFSLoader $1 $2
fi



