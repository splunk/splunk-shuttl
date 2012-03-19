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

import sys,splunk.Intersplunk
import time 


logger = None 
enable_info_log = False 
db_spec_conf    = 'dbspecs'

def initLogger():
    global logger
    if not logger: 
       import splunk.mining.dcutils as dcu
       logger = dcu.getLogger()
    return logger

def log_info(s):
    if enable_info_log:
       initLogger().info("%s - %s" % (sys.argv[0], s))

def log_error(s):
    initLogger().error("%s - %s" % (sys.argv[0], s))

def log_query(query):
    s = query.strip().lower()
    if not enable_info_log and ( s.startswith("select") or s.startswith("insert") ) :
       return
    log_info("executing: %s" % query)

def toInt(s, d):
    try:
        return int(s)
    except:
        return d

def getDefaultDbOptions():
   result = {}
   result['type']                = 'hive'
   result['host']                = 'localhost'
   result['port']                = '10000'
   result['schema']              = 'splunk'
   result['username']            = '' 
   result['password']            = '' 

   return result

def unquote(v):
    if len(v) > 1 and v.startswith('"') and v.endswith('"'):
           return v[1:-1]
    return v

def getArgs(argvals, sessionKey=None, namespace='Hive', owner='nobody'):

    result = getDefaultDbOptions()

    if 'spec' in argvals and len(argvals['spec']) > 0 :
         # try to get the given spec stanza from splunkd 
         from splunk.bundle import getConf 
         conf   = getConf('dbspecs', namespace=namespace, owner=owner, sessionKey=sessionKey)
         stanza = conf.get(unquote(argvals['spec']), None)
         for k,v in stanza.iteritems():
             result[k] = v


    #override any args with the ones specified in the cmd line
    for k,v in argvals.iteritems():
        result[k] = v

    #now unquote any args
    for k,v in result.iteritems():
         result[k] = unquote(v)

    return result

def verifyRequiredArgs(args, presentArgs, nonEmptyArgs):
   for arg in presentArgs:
      if arg not in args:
         raise Exception("Missing required argument, name="+arg)

   for arg in nonEmptyArgs:
      if arg not in args:
         raise Exception("Missing required argument, name="+arg)
    
      if len(args[arg].strip()) == 0:
         raise Exception("Required argument name=%s cannot be an empty or all white space string " %( arg ))

