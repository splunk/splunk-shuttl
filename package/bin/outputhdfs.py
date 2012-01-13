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
import json
import csv

DEFAULT_ARGS = {
	'file': 'nofile',
	'type': 'json'
}

def csvrows(results):
	firstime = True
	for r in results:
		row = []
		header = []
		if (firstime):
			for k in r.keys():
				header.append(k)
			csv.writer(process.stdin).writerow(header)		
			firstime = False
		for k in r.keys():
			row.append(r[k])
		csv.writer(process.stdin).writerow(row)
		
		
def jsonrows(results):
	for r in results:
		row = []
		for k in r.keys():
			field = {}
			field[k] = r[k]
			row.append(field)
		csv.writer(process.stdin).writerow(row)
	
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

process = subprocess.Popen('java -cp $SPLUNK_HOME/etc/apps/shep/lib/hadoop-core-0.20.203.0.jar:$SPLUNK_HOME/etc/apps/shep/lib/commons-logging-1.1.1.jar:$SPLUNK_HOME/etc/apps/shep/lib/commons-configuration-1.6.jar:$SPLUNK_HOME/etc/apps/shep/lib/commons-lang-2.4.jar:$SPLUNK_HOME/etc/apps/shep/bin/splunk-hadoop-connector-0.4.1.jar com.splunk.shep.customsearch.HDFSPut ' + args['file'] + ' ' + args['type'], shell=True, stdin=subprocess.PIPE)
# output results
results,unused1,unused2 = splunk.Intersplunk.getOrganizedResults()

if (args['type'] == 'json'):
	jsonrows(results)
else :
	csvrows(results)
process.stdin.write("exit")
process.stdin.write("\n")

splunk.Intersplunk.outputResults(results)
