import logging

import cherrypy
import urllib
import splunk.rest
import json
import splunk.bundle as bundle
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

from model import Model

logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.Archiving')

class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    # Gives the entire archiver page
    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):

        indexes = json.loads(splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/index/list')[1])
        buckets = json.loads(splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list')[1])
        
        # discard container
        buckets = buckets['bucket']

        return self.render_template('/shep:/templates/archiving.html', dict(indexes=indexes, buckets=buckets)) 

    # Gives a list of buckets for a specific index as an html table
    @expose_page(must_login=True, methods=['GET', 'POST']) 
    def list_buckets(self, **kwargs):
        
        buckets = json.loads(splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/list')[1])
        
        # discard container
        buckets = buckets['bucket']

        return self.render_template('/shep:/templates/bucket_list.html', dict(buckets=buckets))

    # Attempts to thaw buckets in a specific index and time range
    @expose_page(must_login=True, trim_spaces=True, methods=['GET'])
    def thaw(self, index, _from, to, **params):

        index='someindex'
        _from='from'
        to='to'

        params = urllib.urlencode({'index' : index, 'from' : _from, 'to' : to})
        response = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/thaw?%s' % params)
        if response[0]['status']==205:
            return self.render_template('/shep:/templates/success.html')  
        else:
            # TODO: error handling
            return None
