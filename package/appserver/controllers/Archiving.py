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
FAILED_HEADER = collections.OrderedDict([ 
                    ('bucket_bucketName','Name'), ('reason','Reason'), ('bucket_indexName','Index'), ('bucket_format','Format'), 
                    ('bucket_fromDate','From'), ('bucket_toDate','To'), ('bucket_size','Size'), ('bucket_uri','URI') ])


class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    def flatten(self, d, parent_key=''):
        flattenedItems = []
        for k, v in d.items():
            new_key = parent_key + '_' + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                flattenedItems.extend(self.flatten(v, new_key).items())
            else:
                flattenedItems.append((new_key, v))
        return collections.OrderedDict(flattenedItems)

    # Gives the entire archiver page
    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):
        
        errors = None
        
        logger.info('Show archiving page')

        return self.render_template('/shep:/templates/archiving.html', dict(errors=errors))

    # Gives the entire archiver page
    @expose_page(must_login=True, methods=['GET']) 
    def list_indexes(self, **kwargs):
        
        errors = None
        indexes = []
        # may raise exception (ex. connection refused)
        indexesResponse = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/index/list');

        if DEBUG: 
            indexes = debugIndexes
        else:
            # Check http status codes
            if indexesResponse[0].status==200:
                indexes = json.loads(indexesResponse[1], object_pairs_hook=collections.OrderedDict)
                if not indexes: indexes = []
            else:
                # Error hadoop or rest (jetty) problem
                errors = [ "<h1>Got a NON 200 status code!</h1>", 
                    "Index response:", indexesResponse[0], indexesResponse[1] ]

        indexes = sorted(indexes)
        
        logger.debug('show - indexes: %s (%s)' % (indexes, type(indexes)))

        return self.render_template('/shep:/templates/index_list.html', dict(indexes=indexes, errors=errors))

    # Gives a list of buckets for a specific index as an html table
    @expose_page(must_login=True, methods=['POST'])
    def list_buckets(self, **params):
        
        errors = None
        buckets = {}

        logger.debug('list_buckets - postArgs: %s (%s)' % (params, type(params)))

        bucketsResponse = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list', getargs=params)
        logger.debug('list_buckets - response: %s (%s)' % (bucketsResponse, type(bucketsResponse)))

        if DEBUG: 
            time.sleep(2)
            buckets = debugBuckets
        else:
            if bucketsResponse[0].status==200:
                buckets = json.loads(bucketsResponse[1], object_pairs_hook=collections.OrderedDict)
                if not buckets: buckets = {'buckets': {}}
            else:
                errors = [ "<h1>Got a NON 200 status code!</h1>", 
                    "Response header:", bucketsResponse[0], 
                    "Response body:", bucketsResponse[1] ]

        buckets['SUPER_HEADER'] = SUPER_HEADER
        buckets['buckets_NO_DATA_MSG'] = "No buckets in that range!"

        logger.debug('list_buckets - buckets: %s (%s)' % (buckets, type(buckets)))

        return self.render_template('/shep:/templates/bucket_list.html', dict(tables=buckets, errors=errors))

    # Attempts to thaw buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['POST'])
    def thaw(self, **params):
        
        errors = None
        responseData = {}

        logger.debug('thaw - postArgs: %s (%s)' % (params, type(params)))

        response = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/thaw', postargs=params, method='POST')
        
        if DEBUG:
            time.sleep(2)
            responseData = debugFailedThawedBuckets
        else:
            if response[0].status==200:
                responseData = json.loads(response[1], object_pairs_hook=collections.OrderedDict)
                
                # put thawed before failed
                items = responseData.items()
                items.reverse()
                responseData = collections.OrderedDict(items)

                if not responseData: # Should not happen, but if
                    responseData = {}
                    logger.error("thaw - got OK http response but None data")
                    errors = ['Error! Got no data as thaw response!']
                else:
                    responseData['failed'] = map( self.flatten , responseData['failed'] )
            else:
                errors = [ "<h1>Got a NON 200 status code!</h1>", "Response header:", response[0], "Response body:", response[1] ]

        responseData['thawed_TITLE'] = "Thawed buckets:"
        responseData['failed_TITLE'] = "Failed buckets:"
        responseData['SUPER_HEADER'] = SUPER_HEADER
        responseData['failed_HEADER'] = FAILED_HEADER
        responseData['thawed_NO_DATA_MSG'] = "No buckets to thaw!"

        logger.debug('thaw_buckets - buckets: %s (%s)' % (responseData, type(responseData)))

        return self.render_template('/shep:/templates/bucket_list.html', dict(tables=responseData, errors=errors))  

