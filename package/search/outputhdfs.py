# Copyright (C) 2005-2011 Splunk Inc. All Rights Reserved.  Version 4.0
#
# coding=utf-8
#
# outputthdfs - custom search command for use with Hadoop
#
# writes search output results to a file in HDFS
#
# Usage:
#
#   'outputhdfs <file=hdfs file url>' 
#   eg. your_splunk_search | outputhdfs file=hdfs://localhost:54310/myapp/splunkdatainput
#

import subprocess
import splunk.Intersplunk
import sys

DEFAULT_ARGS = {
	'file': 'nofile'
}

# merge any passed args
args = DEFAULT_ARGS
for item in sys.argv:
	kv = item.split('=')
	if len(kv) > 1:
		val = item[item.find('=') + 1:]
		args[kv[0]] = val


if  args['file'] == 'nofile':
    errorresult = splunk.Intersplunk.generateErrorResults("Usage: outputhdfs <file=hdfsfilename>")
    splunk.Intersplunk.outputResults(errorresult)
    sys.exit()

process = subprocess.Popen('java -cp $HADOOP_HOME/hadoop-core-0.20.205.0.jar:$HADOOP_HOME/lib/commons-logging-1.1.1.jar:$HADOOP_HOME/lib/commons-configuration-1.6.jar:$HADOOP_HOME/lib/commons-lang-2.4.jar:$SPLUNK_HOME/etc/apps/shep/bin/splunk-hadoop-connector-0.4.1.jar com.splunk.shep.customsearch.HDFSOut ' + args['file'], shell=True, stdin=subprocess.PIPE)
# output results
results,unused1,unused2 = splunk.Intersplunk.getOrganizedResults()

for r in results:
	process.stdin.write(r["_raw"])
	process.stdin.write("\n")
process.stdin.write("exit")
process.stdin.write("\n")

splunk.Intersplunk.outputResults(results)
