import logging

import cherrypy
import urllib
import json
import splunk.rest
import splunk.bundle as bundle
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.Archiving')
DEBUG = False
defaultBuckets = { 'bucket': [{ 
                'bucketName': "test1", 
                'indexName': "index1", 
                'format': "test format", 
                'uri': "http'://", 
                'fromDate': "2012-05-05", 
                'toDate': "2012-05-06", 
                'size': "1337"
                }, { 
                'bucketName': "test2", 
                'indexName': "index1", 
                'format': "test format", 
                'uri': "http'://", 
                'fromDate': "2012-05-05", 
                'toDate': "2012-05-06", 
                'size': "13"
                }]
            }

class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    # Gives the entire archiver page
    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):
        
        indexes = []
        buckets = {'bucket' : []}
        indexesRequest = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/index/list');
        bucketsRequest = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list');

        # debug
        if DEBUG: 
            buckets = defaultBuckets
        else:
            # Check http status codes
            if indexesRequest[0].status==200 and bucketsRequest[0].status==200:
                indexes = json.loads(indexesRequest[1])
                buckets = json.loads(bucketsRequest[1])
            else:
                # Error hadoop or rest (jetty) problem
                return 'Error could not load index OR buckets'

        # discard container
        if buckets is not None:
            buckets = buckets['bucket']

        logger.error('show - indexes: %s (%s)' % (indexes, type(indexes)))
        logger.error('show - buckets: %s (%s)' % (buckets, type(buckets)))

        return self.render_template('/shep:/templates/archiving.html', dict(indexes=indexes, buckets=buckets))

    # Gives a list of buckets for a specific index as an html table
    @expose_page(must_login=True, methods=['POST'])
    def list_buckets(self, **params):
        
        logger.error('list_buckets - postArgs: %s (%s)' % (params, type(params)))
        buckets = json.loads(splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list', getargs=params)[1])
        logger.error('list_buckets - buckets: %s (%s)' % (buckets, type(buckets)))

        # debug
        if DEBUG: 
            buckets = defaultBuckets

        # discard container
        if buckets is not None:
            buckets = buckets['bucket']

        return self.render_template('/shep:/templates/bucket_list.html', dict(buckets=buckets, data=params))

    # Attempts to thaw buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['GET'])
    def thaw(self, **params):
        index = params['index']
        from_date = params['from']
        to_date = params['to']
        params = urllib.urlencode({'index' : index, 'from' : from_date, 'to' : to_date})
        response = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/thaw?%s' % params)
        if response[0]['status']=='200':
            return self.render_template('/shep:/templates/success.html', dict(buckets=response[1]))  
        else:
            raise Exception('Expected status 200' + str(response))
