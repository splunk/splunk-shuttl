# Copyright (C) 2005-2011 Splunk Inc. All Rights Reserved.  Version 4.0
import subprocess
import os, sys, time, datetime
import splunk.Intersplunk as isp

if len(sys.argv) < 2:
 	errorresult = splunk.Intersplunk.generateErrorResults("Usage: inputhdfs <filename>")
 	splunk.Intersplunk.outputResults(errorresult)
	sys.exit()

process = subprocess.Popen('$HADOOP_HOME/bin/hadoop dfs -cat ' + sys.argv[1], shell=True, stdout=subprocess.PIPE)

results = []
chunkcount = 1;
while True:
	try :
		line = process.stdout.readline()
		if line:
			rowset = {}
			words = line.split(' ')
			colcount = 0
			for word in words:
				rowset['col'+str(colcount)] = word.rstrip()	
				colcount += 1
			rowset['_time']  = float(time.time())
			rectime  = datetime.datetime.fromtimestamp(rowset['_time']).isoformat()	
			raw = []
			raw.append(rectime)
			raw.append(line)
			rowset['_raw'] = ' '.join(raw)
			results.append(rowset)
			chunkcount += 1
			"""
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

