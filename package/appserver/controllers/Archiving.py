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

import os
CONTROLLER_DIR = os.path.abspath(os.path.dirname(__file__))
APPSERVER_DIR = os.path.abspath(os.path.join(CONTROLLER_DIR, '..'))
APP_DIR = os.path.abspath(os.path.join(APPSERVER_DIR, '..'))
BIN_DIR = os.path.abspath(os.path.join(APP_DIR, 'bin'))

import sys
sys.path.append(BIN_DIR)

import shuttl_rest_handler as shuttl

SHUTTL_PORT = shuttl.getShuttlPort()
SHUTTL_URI = "http://localhost:" + SHUTTL_PORT

DEBUG = False
debugIndexes = ['test index 1', 'test index 2']
debugBuckets = { 
    'buckets': [
    { 'bucketName': "test1", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "1337"}, 
    { 'bucketName': "test2", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "13"}
    ]}
debugFailedThawedBuckets = {
    'SUPER_HEADER': {'indexName': 'Index name', 'bucketName': 'Bucket name', 'size': 'Size'},
    'failed_HEADER': {'bucketName': 'Bucket name'},
    'failed': [
    { 'bucketName': "test1", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "1337"}, 
    { 'bucketName': "test2", 'indexName': "index1", 'format': "test format", 'uri': "http'://", 'fromDate': "2012-05-05", 'toDate': "2012-05-06", 'size': "13"} 
    ], 'buckets': [
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

    # Flattens a dictionary of dictionarys
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

        return self.render_template('/shuttl:/templates/archiving.html', dict(errors=errors))

    @expose_page(must_login=True, methods=['GET'])
    def show_flush(self, **kwargs):
        errors = None
        logger.info('Show flushing page')
        return self.render_template('/shuttl:/templates/flushing.html', dict(errors=errors))

    # Gives all indexes that are thawable
    @expose_page(must_login=True, methods=['GET']) 
    def list_indexes(self, **kwargs):
        
        errors = None
        indexes = []
        # may raise exception (ex. connection refused)
        indexesResponse = splunk.rest.simpleRequest(SHUTTL_URI + '/shuttl/rest/archiver/index/list');

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

        indexes = sorted(indexes.get('indexes'))
        
        logger.debug('show - indexes: %s (%s)' % (indexes, type(indexes)))

        return self.render_template('/shuttl:/templates/index_list.html', dict(indexes=indexes, errors=errors))

    # Gives a list of buckets for a specific index as an html table
    @expose_page(must_login=True, methods=['POST'])
    def list_buckets(self, **params):
        return self.list_buckets_at(SHUTTL_URI + '/shuttl/rest/archiver/bucket/list', params)

    @expose_page(must_login=True, methods=['POST'])
    def list_thawed(self, **params):
        return self.list_buckets_at(SHUTTL_URI + '/shuttl/rest/archiver/thaw/list', params)

    def list_buckets_at(self, url, params):

        errors = None
        buckets = {}

        logger.debug('list_buckets - postArgs: %s (%s)' % (params, type(params)))

        bucketsResponse = splunk.rest.simpleRequest(url, getargs=params)
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
        buckets['buckets_TOTAL_SIZE'] = self.bytes_to_size(buckets['buckets_TOTAL_SIZE']) 
        
        buckets['buckets'] = self.set_bucket_sizes_to_human_readable_format(buckets['buckets'])

        logger.debug('list_buckets - buckets: %s (%s)' % (buckets, type(buckets)))
        buckets.pop("exceptions", None)
        return self.render_template('/shuttl:/templates/bucket_list.html', dict(tables=buckets, errors=errors))

    def bytes_to_size(self, num):
        for x in ['bytes','KB','MB','GB', 'TB']:
            if num < 1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0
        return "%3.1f %s" % (num, 'PB')

    def set_bucket_sizes_to_human_readable_format(self, buckets):
        for bucket in buckets:
            bucket['size'] = self.bytes_to_size(bucket['size'])
        return buckets

    # Attempts to flush buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['POST'])
    def flush(self, **params):
        return self.bucket_action_at(SHUTTL_URI + '/shuttl/rest/archiver/bucket/flush', params)

    # Attempts to thaw buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['POST'])
    def thaw(self, **params):
        return self.bucket_action_at(SHUTTL_URI + '/shuttl/rest/archiver/bucket/thaw', params)
        
    def bucket_action_at(self, url, params):
        errors = None
        responseData = {}

        logger.debug('bucket action - postArgs: %s (%s)' % (params, type(params)))

        response = splunk.rest.simpleRequest(url, postargs=params, method='POST')
        
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

        if errors == None:
            return "Success!"
        else:
            return self.render_template('/shuttl:/templates/bucket_list.html', dict(tables=responseData, errors=errors))  

