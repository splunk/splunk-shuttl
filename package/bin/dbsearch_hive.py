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
  | hivequer spec=MyDbSpec query="SELECT * FROM MyTable WHERE age<42" | ... <other splunk commands here> ...


Author: Ledion Bitincka
"""

import csv,sys,hivecommon, splunk.mining.dcutils as dcu
import splunk.Intersplunk as isp

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


def usage():
   print "Usage: %s query=<query> [spec=<spec-name>] [username=<username>] [password=<password>] [schema=<schema>] [host=<host>] [port=<port>]" % (sys.argv[0])
   print "      query      - Required. Database query to execute."
   print "      spec       - Optional. The database spec (stanza name from databases.conf) to use. The values in spec can be overriden by specifying them as args to this script"
   print "      username   - Optional. Username to use when authenticating with the database server"
   print "      password   - Optional. Password to use when authenticating with the database server"
   print "      schema     - Optional. Schema to connect to"
   print "      host       - Optional. Host where the database server is running. Defaults to localhost."
   print "      port       - Optional. Port where the database server is listening on. Defaults to 3306."
   exit(1)    


def streamResults(body_io, outputfile=sys.stdout):
    body_str = ""
    body_str = body_io.getvalue()

    outputfile.write("splunk %s,%u,%u\n" % ("4.3", 0, len(body_str)))
    body_str = body_str.encode("utf-8")
    outputfile.write(body_str)



def main(results, settings, messages):
    keywords, argvals = isp.getKeywordsAndOptions()
    sessionKey = settings.get("sessionKey", None)
    owner      = settings.get("owner",      None)
    namespace  = settings.get("namespace",  None)
    
    output_chunk_size = 64*1024 

    # get the working args, will contact splunkd if a spec key is given
    args   = hivecommon.getArgs(argvals, sessionKey, namespace, owner)
  
    hivecommon.verifyRequiredArgs(args , [], ["host", "port", "schema", "query"])
    query = args['query']
    if query.startswith('"') and query.endswith('"') and len(query) > 1:
       query = query[1:-1]
   
    # serialize the results into the output buffer as we read them
    body_io = StringIO()
    w = csv.writer(body_io)
    result_count = 0

    try:
        transport = TSocket.TSocket(args['host'], int(args['port']))
        transport = TTransport.TBufferedTransport(transport)
        protocol  = TBinaryProtocol.TBinaryProtocol(transport)
    
        client = ThriftHive.Client(protocol)
        transport.open()
       

        # execute query
        if 'schema' in args and len(args['schema']) >0:
            client.execute("USE " + args['schema'])

        client.execute(query)


        # get result schema
        schema  = client.getSchema()
        columns =  []
        if schema and schema.fieldSchemas:
           for i in schema.fieldSchemas:
             columns.append(i.name)

        w.writerow(columns)

        while True:
           try:
             row = client.fetchOne()
             if (row == None):
                 break
           except HiveServerException, hse:
               if hse.errorCode == 0:
                   break
         
           # hive uses tab-delimiter, splunk only accepts csv
           # parse the tsv here - kinda wierd for an API to spew out encoded results
           parts = row.split('\t')
           for i in xrange(len(parts)):
              parts[i] = parts[i].replace('\\t', '\t')
          
           # write the result out - no need to keep it around
           w.writerow(parts)  
           result_count += 1   

           # check to see if we need to flush the buffer out
           if body_io.tell() >= output_chunk_size:
              streamResults(body_io)
              body_io.truncate(0)
              result_count = 0
              w.writerow(columns)

        # flush any remaining result
        if result_count > 0 :
          streamResults(body_io)

    except Thrift.TException, tx:
        raise Exception("Error while executing: %s. Message: %s" % (query, tx.message))
    finally:
        transport.close()
        body_io.close() 
   
results, dummyresults, settings = isp.getOrganizedResults()
messages = {}

try:
   main(results, settings, messages)
   sys.exit(0)
except Exception, e:
   hivecommon.log_error(str(e))
   isp.addErrorMessage(messages, str(e))
   results = []
   isp.outputResults(results, messages)


