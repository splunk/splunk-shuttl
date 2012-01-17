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
#   when the append is set to True rows from the hdfs file or from the job output is appended to the piped in results
#   max specifies the maximum number of rows that is output
#

import subprocess
import os, sys, time, datetime
import splunk.Intersplunk as isp
import splunk.util as util

DEFAULT_ARGS = {
    'separator': 'space',
    'file': 'nofile',
    'append': False,
    'max': -1,
    'input': 'noinput',
    'output': 'nooutput',
    'job': 'nojob'
}

# merge any passed args
args = DEFAULT_ARGS
for item in sys.argv:
    kv = item.split('=')
    if len(kv) > 1:
        val = item[item.find('=') + 1:]
        try:
            val = int(val)
        except:
            pass
        args[kv[0]] = util.normalizeBoolean(val)

HADOOP_CLIENT_JARS='../lib/hadoop-core-0.20.203.0.jar:../lib/commons-logging-1.1.1.jar:../lib/commons-configuration-1.6.jar:../lib/commons-lang-2.4.jar'
HADOOP_LAUNCH_JARS='../lib/commons-httpclient-3.1.jar:../lib/jackson-core-asl-1.4.0.jar:../lib/jackson-mapper-asl-1.4.0.jar:../lib/log4j-1.2.16.jar'
SPLUNK_JARS='./*'

def catfile(filename):
    process = subprocess.Popen('java -cp ' + HADOOP_CLIENT_JARS + ':' + SPLUNK_JARS + ' com.splunk.shep.customsearch.HDFSCat ' + filename, shell=True, stdout=subprocess.PIPE)
    return process

if args['file'] == 'nofile' and args['job'] == 'nojob':    
    errorresult = isp.generateErrorResults("Usage: inputhdfs <separator=space|tab> <file=hdfsfilename | job=mapredclassname> <max=#rows> <append=True|False>")
    isp.outputResults(errorresult)
    sys.exit()

separator_char = ' '

if args['separator'] == 'tab':
    separator_char = '\t'
else :
    separator_char = ','
    
if (args['file']) != 'nofile':
    process = catfile(args['file'])
if (args['job']) != 'nojob':
    process = subprocess.Popen('java -cp ' + HADOOP_CLIENT_JARS + ':' + HADOOP_LAUNCH_JARS + ':' + SPLUNK_JARS +  ' ' 
                                    + args['job'] + ' ' + args['input'] + ' ' + args['output'], shell=True)
    exitstatus = process.wait()
    if (exitstatus == 0):
        process = catfile(args['output']+'/part-00000')

results = []
if (args['append'] == True):
    results,unused1,unused2 = isp.getOrganizedResults()
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
            if ((args['max'] != -1) and (chunkcount > args['max'])):
                isp.outputResults(results)
                break
#            works only in Splunk 4.3
#            if chunkcount == 5:
#                isp.outputStreamResults(results)
#                chunkcount = 0
#                results = []
        else :
#            works only in 4.3
#            if chunkcount > 0:
#                isp.outputStreamResults(results)
            isp.outputResults(results)
            sys.exit(0)
    except IOError:
        sys.exit(1)
sys.exit(0)
    
