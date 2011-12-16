# Copyright (C) 2005-2011 Splunk Inc. All Rights Reserved.  Version 4.0
#
# coding=utf-8
#
# inputhdfs - custom search command for use with Hadoop
#
# Retrieves textual data from a file in HDFS
#
# Usage:
#
#   At the most basic level, you can just do 'inputhdfs <hdfs file url>.  The arguments
#   listed below in DEFAULT_ARGS can all be overridden as search arguments in
#   the search string, i.e., 'inputhdfs separator=space file=hdfs://localhost:54310/myapp/outputtxtfile'.  From the
#   current UI, prefix the search string with a pipe, i.e. '| inputhdfs <hdfs file url>'
#
#   append and max  options are not yet implemented
#

import subprocess
import os, sys, time, datetime
import splunk.Intersplunk as isp

DEFAULT_ARGS = {
    'separator':	'space',
	'file': 'nofile'
}

# merge any passed args
args = DEFAULT_ARGS
for item in sys.argv:
	kv = item.split('=')
	if len(kv) > 1:
		val = item[item.find('=') + 1:]
		args[kv[0]] = val


if args['file'] == 'nofile':	
 	errorresult = splunk.Intersplunk.generateErrorResults("Usage: inputhdfs <separator=space|tab> <file=hdfsfilename>")
 	splunk.Intersplunk.outputResults(errorresult)
	sys.exit()

separator_char = ' '

if args['separator'] == 'tab':
	separator_char = '\t'

process = subprocess.Popen('$HADOOP_HOME/bin/hadoop dfs -cat ' + args['file'], shell=True, stdout=subprocess.PIPE)

results = []
chunkcount = 1;
while True:
	try :
		line = process.stdout.readline()
		if line:
			rowset = {}
			words = line.rstrip().split(separator_char)
			rowset['_time']  = float(time.time())
			rectime  = datetime.datetime.fromtimestamp(rowset['_time']).isoformat()	
			raw = []
			raw.append(rectime)
			colcount = 0
			for word in words:
				rowset['col'+str(colcount)] = word.rstrip()	
				colcount += 1
				raw.append(word.rstrip())
			rowset['_raw'] = separator_char.join(raw)
			results.append(rowset)
			chunkcount += 1
			""" works only in Splunk 4.3
			if chunkcount == 5:
				isp.outputStreamResults(results)
				chunkcount = 0
				results = []
			"""
		else :
			"""
			if chunkcount > 0:
				isp.outputStreamResults(results)
			"""
			isp.outputResults(results)
			sys.exit()
	except IOError:
		sys.exit()
	
