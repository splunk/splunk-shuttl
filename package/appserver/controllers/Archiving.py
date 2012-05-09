import logging

import cherrypy
import splunk.rest
import splunk.bundle as bundle
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

import urllib
import json
import time
import collections


DEBUG = False
debugIndexes = ['test index 1', 'test index 2']
debugBuckets = { 
    'bucket': [
    { 'bucketName': "test1", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "1337"}, 
    { 'bucketName': "test2", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "13"}
    ]}
debugFailedThawedBuckets = {
    'SUPER_HEADER': {'indexName': 'Index name', 'bucketName': 'Bucket name', 'size': 'Size'},
    'failed_HEADER': {'bucketName': 'Bucket name'},
    'failed': [
    { 'bucketName': "test1", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "1337"}, 
    { 'bucketName': "test2", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "13"} 
    ], 'thawed': [
    { 'bucketName': "test1", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "1337"}, 
    { 'bucketName': "test2", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "13"}
    ]}


logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.Archiving')
SUPER_HEADER = collections.OrderedDict([ 
                    ('bucketName','Name'), ('indexName','Index'), ('format','Format'), 
                    ('fromDate','From'), ('toDate','To'), ('size','Size'), ('uri','URI') ])


class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    # Gives the entire archiver page
    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):
        
        errors = None
        indexes = []
        buckets = {}
        indexesResponse = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/index/list');
        bucketsResponse = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list');

        if DEBUG: 
            indexes = debugIndexes
            buckets = debugBuckets
        else:
            # Check http status codes
            if indexesResponse[0].status==200 and bucketsResponse[0].status==200:
                indexes = json.loads(indexesResponse[1], object_pairs_hook=collections.OrderedDict)
                buckets = json.loads(bucketsResponse[1], object_pairs_hook=collections.OrderedDict)
            else:
                # Error hadoop or rest (jetty) problem
                errors = [ "<h1>Got a NON 200 status code!</h1>", 
                    "Index response:", indexesResponse[0], indexesResponse[1],
                    "Bucket response:", bucketsResponse[0], bucketsResponse[1] ]

        indexes = sorted(indexes)
        buckets['SUPER_HEADER'] = SUPER_HEADER

        logger.error('show - indexes: %s (%s)' % (indexes, type(indexes)))
        logger.error('show - buckets: %s (%s)' % (buckets, type(buckets)))

        return self.render_template('/shep:/templates/archiving.html', dict(indexes=indexes, tables=buckets, errors=errors))

    # Gives a list of buckets for a specific index as an html table
    @expose_page(must_login=True, methods=['POST'])
    def list_buckets(self, **params):
        
        errors = None
        buckets = {}

        logger.error('list_buckets - postArgs: %s (%s)' % (params, type(params)))

        bucketsResponse = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list', getargs=params)

        if DEBUG: 
            time.sleep(2)
            buckets = debugBuckets
        else:
            if bucketsResponse[0].status==200:
                buckets = json.loads(bucketsResponse[1], object_pairs_hook=collections.OrderedDict)
            else:
                errors = [ "<h1>Got a NON 200 status code!</h1>", 
                    "Response header:", bucketsResponse[0], 
                    "Response body:", bucketsResponse[1] ]

        buckets['SUPER_HEADER'] = SUPER_HEADER

        logger.error('list_buckets - buckets: %s (%s)' % (buckets, type(buckets)))

        return self.render_template('/shep:/templates/bucket_list.html', dict(data=params, tables=buckets, errors=errors))

    # Attempts to thaw buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['GET'])
    def thaw(self, **params):
        
        errors = None
        responseData = {}

        index = params['index']
        from_date = params['from']
        to_date = params['to']
        params = urllib.urlencode({'index' : index, 'from' : from_date, 'to' : to_date})
        response = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/thaw?%s' % params)
        
        if DEBUG:
            time.sleep(2)
            responseData = debugFailedThawedBuckets
        else:
            if response[0].status==200:
                responseData = json.loads(response[1], object_pairs_hook=collections.OrderedDict)
            else:
                errors = [ "<h1>Got a NON 200 status code!</h1>", "Response header:", response[0], "Response body:", response[1] ]

        responseData['thawed_TITLE'] = "Thawed buckets:"
        responseData['failed_TITLE'] = "Failed buckets:"

        logger.error('thaw_buckets - buckets: %s (%s)' % (responseData, type(responseData)))

        return self.render_template('/shep:/templates/bucket_list.html', dict(tables=responseData, errors=errors))  

