import logging

import cherrypy
import urllib
import splunk.rest
import splunk.bundle as bundle
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

from model import Model

logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.Archiving')

class Archiving(controllers.BaseController):
    '''Archiving Controller'''

    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):

        buckets = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/list/buckets')[1]
        
        indexes  = ['foo','bar'] 
        user = cherrypy.session['user']['name']

        return self.render_template('/shep:/templates/archiving.html', dict(indexes=indexes, buckets=buckets)) 

    @expose_page(must_login=True, methods=['GET', 'POST']) 
    def list_buckets(self, **kwargs):
        
        buckets = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/list/buckets')[1]
        
        return self.render_template('/shep:/templates/bucket_list.html', dict(buckets=buckets))

    @expose_page(must_login=True, trim_spaces=True, methods=['GET']) 
    def thaw(self, **params):

        index='someindex'
        path='somebucketpath'

        params = urllib.urlencode({'index' : index, 'path' : path})
        result = splunk.rest.simpleRequest('http://localhost:9090/shep/rest/archiver/bucket/archive?%s' % params)

        return self.render_template('/shep:/templates/success.html') 

#    def simpleRequest(path, sessionKey=None, getargs=None, postargs=None, method='GET', raiseAllErrors=False, proxyMode=False, rawResult=False, timeout=SPLUNKD_CONNECTION_TIMEOUT, jsonargs=None):
#        """
#        Makes an HTTP call to the main splunk REST endpoint
#        
#        path: the URI to fetch
#            If given a relative URI, then the method will normalize to the splunkd
#            default of "/services/...".
#            If given an absolute HTTP(S) URI, then the method will use as-is.
#            If given a 'file://' URI, then the method will attempt to read the file
#            from the local filesystem.  Only files under $SPLUNK_HOME are supported,
#            so paths are 'chrooted' from $SPLUNK_HOME.
#            
#        getargs: dict of k/v pairs that are always appended to the URL
#        
#        postargs: dict of k/v pairs that get placed into the body of the 
#            request. If postargs is provided, then the HTTP method is auto
#            assigned to POST.
#            
#        method: the HTTP verb - [GET | POST | DELETE | PUT]
#        
#        raiseAllErrors: indicates if the method should raise an exception
#            if the server HTTP response code is >= 400
#    
#        rawResult: don't raise an exception if a non 200 response is received;
#            return the actual response
#        
#        Return:
#        
#            This method will return a tuple of (serverResponse, serverContent)
#            
#            serverResponse: a dict of HTTP status information
#            serverContent: the body content
#        """
#        
#        # if absolute URI, pass along as-is
#        if path.startswith('http'):
#            uri = path
#            
#        # if file:// protocol, try to read file and return
#        # the serverStatus is just an empty dict; file contents are in serverResponse
#        # TODO: this probably doesn't work in windows
#        elif path.startswith('file://'):
#            workingPath = path[7:].strip(os.sep)
#            lines = util.readSplunkFile(workingPath)
#            return ({}, ''.join(lines))
#                
#        else:
#            # prepend convenience root path
#            if not path.startswith(REST_ROOT_PATH): path = REST_ROOT_PATH + '/' + path.strip('/')
#            
#            # setup args
#            host = splunk.getDefault('host')
#            if ':' in host:
#                host = '[%s]' % host
#                
#            uri = '%s://%s:%s/%s' % \
#                (splunk.getDefault('protocol'), host, splunk.getDefault('port'), path.strip('/'))
#    
#        if getargs:
#            getargs = dict([(k,v) for (k,v) in getargs.items() if v != None])
#            uri += '?' + util.urlencodeDict(getargs)
#    
#        
#        # proxy mode bypasses all header passing
#        headers = {}
#        sessionSource = 'direct'
#        if not proxyMode:
#            
#            # get session key from known places: first the appserver session, then
#            # the default instance cache
#            if not sessionKey:
#                sessionKey, sessionSource = splunk.getSessionKey(return_source=True)
#            headers['Authorization'] = 'Splunk %s' % sessionKey
#    
#        payload = ''
#        if postargs or jsonargs and method in ('GET', 'POST', 'PUT'):
#            if method == 'GET':
#                method = 'POST'
#            if jsonargs:
#                # if a JSON body was given, use it for the payload and ignore the postargs
#                payload = jsonargs
#            else:
#                payload = util.urlencodeDict(postargs)
#        #
#        # make request
#        #
#        if logger.level <= logging.DEBUG:
#            if uri.lower().find('login') > -1:
#                logpayload = '[REDACTED]'
#            else:
#                logpayload = payload
#            #logger.debug('simpleRequest >>>\n\tmethod=%s\n\turi=%s\n\tbody=%s' % (method, uri, logpayload))
#            logger.debug('simpleRequest > %s %s [%s] sessionSource=%s' % (method, uri, logpayload, sessionSource))
#            t1 = time.time()
#    
#        # Add wait and tries to check if the HTTP server is up and running
#        tries = 4
#        wait = 10
#        try:
#            for aTry in range(tries):
#                h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=True)
#                if WEB_KEYFILE and WEB_CERTFILE:
#                    h.add_certificate(WEB_KEYFILE, WEB_CERTFILE, '')
#                serverResponse, serverContent = h.request(uri, method, headers=headers, body=payload)
#                if serverResponse == None:
#                    if aTry < tries:
#                        time.sleep(wait)
#                else:
#                    break
#        except socket.error, e:
#            raise splunk.SplunkdConnectionException, str(e)
#        except socket.timeout, e:
#            raise splunk.SplunkdConnectionException, 'Timed out while waiting for splunkd daemon to respond. Splunkd may be hung. (timeout=%s)' % SPLUNKD_CONNECTION_TIMEOUT
#        except AttributeError, e:
#            raise splunk.SplunkdConnectionException, 'Unable to establish connection with splunkd deamon. (%s)' % e
#    
#    #    try:
#    #        h = httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=True)
#    #        serverResponse, serverContent = h.request(uri, method, headers=headers, body=payload)
#    #    except socket.error, e:
#    #        raise splunk.SplunkdConnectionException, str(e)
#    #    except socket.timeout, e:
#    #        raise splunk.SplunkdConnectionException, 'Timed out while waiting for splunkd daemon to respond. Splunkd may be hung. (timeout=%s)' % SPLUNKD_CONNECTION_TIMEOUT
#    #    except AttributeError, e:
#    #        raise splunk.SplunkdConnectionException, 'Unable to establish connection with splunkd daemon. (%s)' % e
#    
#        serverResponse.messages = []
#        
#        if logger.level <= logging.DEBUG:
#            logger.debug('simpleRequest < server responded status=%s responseTime=%.4fs' % (serverResponse.status, time.time() - t1))
#            
#        # Don't raise exceptions for different status codes or try and parse the response
#        if rawResult:
#            return serverResponse, serverContent
#    
#        #
#        # we only throw exceptions in limited cases; for most HTTP errors, splunkd
#        # will return messages in the body, which we parse, so we don't want to
#        # halt everything and raise exceptions; it is up to the client to figure 
#        # out the best course of action
#        #
#        if serverResponse.status == 401:
#            #SPL-20915
#            logger.debug('simpleRequest - Authentication failed; sessionKey=%s' % sessionKey)
#            raise splunk.AuthenticationFailed
#        
#        elif serverResponse.status == 402:
#            raise splunk.LicenseRestriction
#        
#        elif serverResponse.status == 403:
#            raise splunk.AuthorizationFailed(extendedMessages=uri)
#            
#        elif serverResponse.status == 404:
#            
#            # Some 404 reponses, such as those for expired jobs which were originally
#            # run by the scheduler return extra data about the original resource.
#            # In this case we add that additional info into the exception object
#            # as the resourceInfo parameter so others might use it.
#            try:
#                body = et.fromstring(serverContent)
#                resourceInfo = body.find('dict')
#                if resourceInfo is not None:
#                    raise splunk.ResourceNotFound(uri, format.nodeToPrimitive(resourceInfo))
#                else:
#                    raise splunk.ResourceNotFound(uri, extendedMessages=extractMessages(body))
#            except et.XMLSyntaxError:
#                pass
#            
#            raise splunk.ResourceNotFound, uri
#        
#        elif serverResponse.status == 201:
#            try:
#                body = et.fromstring(serverContent)
#                serverResponse.messages = extractMessages(body)
#            except et.XMLSyntaxError, e:
#                # do nothing, just continue, no messages to extract if there is no xml
#                pass
#            except e:
#                # warn if some other type of error occurred.
#                logger.warn("exception trying to parse serverContent returned from a 201 response.")
#                pass
#            
#        elif serverResponse.status < 200 or serverResponse.status > 299:
#            
#            # service may return messages in the body; try to parse them
#            try:
#                body = et.fromstring(serverContent)
#                serverResponse.messages = extractMessages(body)
#            except:
#                pass
#                
#            if raiseAllErrors and serverResponse.status > 399:
#                
#                if serverResponse.status == 500:
#                    raise splunk.InternalServerError, (None, serverResponse.messages)
#                elif serverResponse.status == 400:
#                    raise splunk.BadRequest, (None, serverResponse.messages)
#                else:
#                    raise splunk.RESTException, (serverResponse.status, serverResponse.messages)
#                
#    
#        # return the headers and body content
#        return serverResponse, serverContent
