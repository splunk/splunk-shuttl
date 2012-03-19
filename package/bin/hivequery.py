# 
# NOTE: THIS IS DEAD CODE. SHOULD BE DELETED - boris
#
# Copyright (C) 2005-2011 Splunk Inc. All Rights Reserved.  Version 4.0
#
# coding=utf-8
#


import os, sys, time, datetime


import splunk.Intersplunk as isp
import splunk.util as util
import splunk.appserver.mrsparkle.lib.util as app_util

APP_NAME ='shep'

local_path = os.path.join(app_util.get_apps_dir(), APP_NAME, 'bin', 'hivepylib081')

if not local_path in sys.path:
    sys.path.append(local_path)

from hive_service import ThriftHive
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

DEFAULT_ARGS = {
    'query': 'select 1',
    'host': 'localhost',
    'port': 10000
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
        
results = []

try:
    transport = TSocket.TSocket(args['host'], args['port'])
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHive.Client(protocol)
    transport.open()
    client.execute(args['query'])
    while (1) :
        row = client.fetchOne()
        if (row == None):
            break
        rowset = {}
        rowset['_time']  = float(time.time())
        rowset['_raw'] = row
        results.append(rowset)
    isp.outputResults(results)

    transport.close()

except Exception, tx:
    if (tx.errorCode == 0) :
        isp.outputResults(results)
    else :
        print '%s' % (tx.message)
