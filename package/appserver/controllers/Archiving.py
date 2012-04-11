import logging

import cherrypy
import splunk.bundle as bundle
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

from model import Model

logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.Archiving')

class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):

        #TODO: REST call
        indexes  = [] 
        user = cherrypy.session['user']['name']

        return self.render_template('/shep:/templates/archiving.html') 
    
    @expose_page(must_login=True, trim_spaces=True, methods=['POST']) 
    def get_buckets(self, **kwargs):
        
        #TODO: REST call
        buckets = []

        return self.render_template('/shep:/templates/search.html', buckets)

    @expose_page(must_login=True, trim_spaces=True, methods=['POST']) 
    def thaw(self, **params):

        #TODO: REST call
        return self.render_template('/webintelligence:/templates/success.html') 
