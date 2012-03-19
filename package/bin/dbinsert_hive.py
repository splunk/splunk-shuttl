# Copyright 2012 Splunk, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


"""
External search command for querying Hive. 

Usage:
  | hiveoutput  spec=MyDbSpec table=MyHiveTable


Author: Ledion Bitincka
"""
import urllib,os,re,time,csv,sys,hivecommon, splunk.mining.dcutils as dcu
import splunk.Intersplunk as isp
import subprocess
APP_NAME ='shep'
local_path = os.path.join(app_util.get_apps_dir(), APP_NAME, 'bin', 'hivepylib081')

if not local_path in sys.path:
    sys.path.append(local_path)

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO


#TODO: update usage
def usage():
   print "Usage: %s table=<table-name> [spec=<spec-name>] [username=<username>] [password=<password>] [schema=<schema>] [host=<host>] [port=<port>]" % (sys.argv[0])
   print "      spec       - Optional. The database spec (stanza name from databases.conf) to use. The values in spec can be overriden by specifying them as args to this script"
   print "      username   - Optional. Username to use when authenticating with the database server"
   print "      password   - Optional. Password to use when authenticating with the database server"
   print "      schema     - Optional. Schema to connect to"
   print "      host       - Optional. Host where the database server is running. Defaults to localhost."
   print "      port       - Optional. Port where the database server is listening on. Defaults to 10000."
   exit(1)    


class SplunkResultStreamer:
   def __init__(self, handler):
      self.settings = {}
      self.header   = []
      self.handler  = handler
      pass

   def run(self):
       #1. read settings
       for line in sys.stdin:
          if line == '\n':
              break
          parts = line.strip().split(':', 2)
          if len(parts) != 2:
             continue
          parts[1] = urllib.unquote(parts[1])
          self.settings[parts[0]] = parts[1]

       #TODO: parse authString XML
 
       self.handler.handleSettings(self.settings)
   
        
       cr = csv.reader(sys.stdin)
       cw = csv.writer(sys.stdout)

       try:
	       self.header = cr.next()
	   
	       #2. csv: read header 
	       out_header = self.handler.handleHeader(self.header)
	       if out_header != None:
		  sys.stdout.write('\n')  # end output header section
		  cw.writerow(out_header)


	       #3. csv: read input results 
	       for row in cr:
		  out_row = self.handler.handleResult(row)  
		  if out_row != None and out_header != None:
		      cw.writerow(out_row)

       except StopIteration, sp:
          pass
           
       #4. get any rows withheld by the handle until it sees finish
       out_rows = self.handler.handleFinish()
       if out_rows != None and out_header != None:
          for out_row in out_rows:
              cw.writerow(out_row)



 
def readCSVDict(path):
    f = open(path, 'r')
    r = csv.DictReader(f)
    info =  r.next() 
    f.close()
    return info

def writeCSVDict(d, path):
    tmp_path = path + '.tmp'
    f = open(tmp_path, 'w')
    w = csv.DictWriter(f, d.keys())
    w.writeheader()
    w.writerow(d)
    f.close()
    os.rename(tmp_path, path)


# cache the hive info so we don't have to for every chunk
def createHiveInfoFile(args, hive_info_file):
    try:
        transport = TSocket.TSocket(args['host'], int(args['port']))
        transport = TTransport.TBufferedTransport(transport)
        protocol  = TBinaryProtocol.TBinaryProtocol(transport)

        client = ThriftHive.Client(protocol)
        transport.open()

        if 'schema' in args and len(args['schema']) >0:
            client.execute("USE " + args['schema'])

        table = client.get_table('default', args['table'])

        # change table location to point to correct server
        if args['host'] != 'localhost' and table.sd.location.startswith('hdfs://localhost:'):
            table.sd.location = 'hdfs://' + args['host'] + table.sd.location.substr(len('hdfs://localhost:'))

        cols_str = []
        for c in table.sd.cols:
            cols_str.append(c.name)

        result = {}
        result['cols']      = ';'.join(cols_str)
        result['hdfs_path'] = table.sd.location

        writeCSVDict(result, hive_info_file)
    except Thrift.TException, tx:
        raise Exception("Error while updating table: %s. Message: %s" % (args['table'], tx.message))
    finally:
        transport.close()

def readHiveInfo(hive_info_file):
    result         = readCSVDict(hive_info_file)
    result['cols'] = result['cols'].split(';')
    return result

def copy_to_hadoop(src, dst):
   hadoop  = os.path.join(os.getenv("HADOOP_HOME"), "bin", "hadoop")
   process = subprocess.Popen([hadoop, 'dfs', '-put', src, dst], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
   output  = process.communicate(None)
   return process.returncode

# 1. write the results into a temporary file
# 2. when the file grows too big or this is the last time we're being called
#    ship the data over to hadoop
class SplunkResultHandler:
    def __init__(self):
       pass

    def handleSettings(self, settings):
	    keywords, argvals = isp.getKeywordsAndOptions()
	    sessionKey = settings.get("sessionKey", None)
	    owner      = settings.get("owner",      None)
	    namespace  = settings.get("namespace",  None)

	    infoPath   = settings.get('infoPath', '')
	    self.info  = readCSVDict(infoPath)

	    search_id      = self.info['_sid']
	    dispatch_dir   = os.path.join(settings.get('sharedStorage', isp.splunkHome()), 'var', 'run', 'splunk', 'dispatch', search_id)

	    if not os.getenv("HADOOP_HOME"):
	       raise Exception("$HADOOP_HOME is not defined. Please define $HADOOP_HOME such that $HADOOP_HOME/bin/hadoop is a valid path")


	    hive_tmp_dir = os.path.join(dispatch_dir, "hive_tmp")
	    if not os.path.exists(hive_tmp_dir):
		os.makedirs(hive_tmp_dir)

	    tsv = ''
	    for file in os.listdir(hive_tmp_dir):
		if file.endswith('.tsv') and file.startswith(search_id):
		   tsv = file
		   break

	    if len(tsv) == 0:
		tsv = "%s_%.3f.tsv" % (search_id, time.time())
	    self.hive_tmp_file  = os.path.join(hive_tmp_dir, tsv )

	    hive_info_file = os.path.join(hive_tmp_dir, 'hive_info.csv')
	    if not os.path.exists(hive_info_file):
	       # get the working args, will contact splunkd if a spec key is given
	       args = hivecommon.getArgs(argvals, sessionKey, namespace, owner)
	       hivecommon.verifyRequiredArgs(args , [], ["host", "port", "schema", "table"])
	       createHiveInfoFile(args, hive_info_file)

 	    self.hive_info = readHiveInfo(hive_info_file)
            self.out = None

    def handleHeader(self, header):
       self.header = header
       self.col_indexes = []
       for c in self.hive_info['cols']:
            try:
               self.col_indexes.append(self.header.index(c))
            except:
               self.col_indexes.append(-1)       
       return header
 
    def handleResult(self, result):
       if self.out == None:
          self.out = open(self.hive_tmp_file, 'a')

       vals = []
       for ci in self.col_indexes:
           if ci == -1:
              vals.append('')
           else:
              vals.append(result[ci])

       self.out.write('\t'.join(vals))
       self.out.write('\n')

       return result

    def handleFinish(self):
            if self.out != None:
               self.out.close()

	    query_finished = int(self.info['_query_finished'])
	    # if file has grown too big or this is the last time we'll be called flush the data to hadoop
	    if os.path.exists(self.hive_tmp_file) and (os.path.getsize(self.hive_tmp_file) > 4*1024*1024 or not query_finished == 0):
		dst = self.hive_info['hdfs_path'] + '/' + os.path.basename(self.hive_tmp_file)
		hivecommon.log_error("copying %s to %s" % (self.hive_tmp_file, dst))
		rv = copy_to_hadoop(self.hive_tmp_file, dst)
		#TODO: report stdout/err from hadoop
		if rv != 0:
		   raise Exception("Could not copy data to hadoop, underlying process exited with rv=%s" % (str(rv)))
		else:
		   os.remove(self.hive_tmp_file)


def stream_in_stream_out():
  srh = SplunkResultHandler()
  srs = SplunkResultStreamer(srh)

  srs.run()

stream_in_stream_out()
   


