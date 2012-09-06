import logging
import collections

import cherrypy
import splunk.bundle as bundle
import splunk.entity
import splunk.appserver.mrsparkle.controllers as controllers
from splunk.appserver.mrsparkle.lib.decorators import expose_page

from splunk.models.field import BoolField
from index import Index


logger = logging.getLogger('splunk.appserver.mrsparkle.controllers.shuttl.Setup')

required_keys = ['web-traffic', 'clientip-internal', 'internal-domain',
                 'brand-name', 'exclude-pageview']

APP                 = 'shuttl'
USER                = 'nobody'
INDEXES             = 'conf-indexes'
SERVICE_ENDPOINT    = '/servicesNS/{owner}/{namespace}'
INDEXES_ENDPOINT    = SERVICE_ENDPOINT + '/configs/conf-indexes/{index}'
S_CHR               = '.'

class Setup(controllers.BaseController):
    '''Shuttl Setup Controller'''

    @expose_page(must_login=True, methods=['GET']) 
    def show(self, **kwargs):

        form_content  = collections.OrderedDict() 
        user = cherrypy.session['user']['name'] or USER
        logger.info('setup/show user=%s' % (user))
        
        index_entities = splunk.entity.getEntities('configs/conf-indexes', namespace=APP, owner=user)
        indexes = collections.OrderedDict()
        
        for key, index_entity in index_entities.iteritems():
            logger.debug('index entity unique_key=%s index_entity=%r' % (key, index_entity))
            index = Index.get(INDEXES_ENDPOINT.format(owner=user, namespace=APP, index=key))
            indexes[key] = index
        
        form_content[INDEXES] = indexes
        
        return self.render_template('/shuttl:/templates/setup.html', 
                                    dict(form_content=form_content))

    @expose_page(must_login=True, trim_spaces=True, methods=['POST']) 
    def save(self, **params):

        form_content = collections.OrderedDict()
        user = cherrypy.session['user']['name'] or USER
        
        logger.debug('params=%r' % params)
        # Parse input names. name="<conf-file-name>.<stanza-name>.<field-name>"
        parsed_params = self._parse_params(params, only_keep_last_value=True)
        
        # Get and update entities.
        for conf_name, stanzas in parsed_params.iteritems():
            if conf_name == 'conf-indexes':
                form_content[conf_name] = collections.OrderedDict()
                for stanza_name, stanza in stanzas.iteritems():
                    try:
                        if stanza_name == '_new':  # Create new index.
                            # This part goes deep first to get last two values, (key, value).
                            for key, value in stanza.iteritems():
                                stanza_name = key
                                stanza = value
                            Index.create_template(stanza_name, namespace=APP, owner='nobody') 
                        
                        form_content[conf_name][stanza_name] = index =\
                            Index.get(INDEXES_ENDPOINT.format(
                                owner=user, namespace=APP, index=stanza_name))
                        
                        logger.debug('form_content name=%s owner=%s sharing=%s' %
                            (index.name, index.owner, index.metadata.sharing))
                        # Always save app changes to app-local dir.
                        index.owner = 'nobody'  # Remove this to save in 'etc/users/<owner>/shuttl/local/indexes.conf'
                        #index.share_app() 

                        for field_name, value in stanza.iteritems():
                            logger.debug('field=%s value=%s' % (field_name, value))
                            # Special case for booleans.
                            if isinstance(Index.__dict__.get(field_name), BoolField) \
                            and field_name=='is_disabled':
                                should_disable = value.lower() in ['1', 'true', 'yes', 'on', 'checked']

                                # Special case for enable/disable
                                if not index.is_disabled and should_disable:
                                    index.disable()
                                if index.is_disabled and not should_disable:
                                    index.enable()
                            else:
                                setattr(index, field_name, value)
                    except Exception, ex:
                        logger.error(ex)
                        logger.error('Failed to load model for index_name=%s fields=%r' % (stanza_name, stanza))
                        return self.render_template('shuttl:/templates/failure.html',
                            dict(conf_name=conf_name, stanza_name=stanza_name, stanza=stanza, exception=ex))

        # Try to save, or else give the form back with prepopulated values
        for conf_name in form_content.keys():
            for stanza in form_content[conf_name].keys():
                if not form_content[conf_name][stanza].passive_save():
                    return self.render_template('/shuttl:/templates/setup.html', 
                        dict(conf_name=conf_name, stanza_name=stanza_name, 
                             form_content=form_content))
      
        # Work around for SPL-40214
        app = bundle.getConf('app', namespace=APP, owner='nobody') 
        app['install']['is_configured'] = 'true'
        logger.info('Save successful')
        return self.render_template('/shuttl:/templates/success.html') 
    
    def _parse_params(self, params, only_keep_last_value=True):
        form_content = collections.OrderedDict()
        
        for k, v in params.iteritems():
            try:
                keys = k.split(S_CHR)
                logger.debug('split_key=%s' % keys)
            except:
                keys = []
                logger.warning("Couldn't parse form content, keys_str=%s" % k)
            
            '''
            content = {
                'conf-index': {
                    'index1': {
                        'name': '1',
                        'desc': 'i1',
                    },
                    'index2': {
                        'name': '2',
                        'desc': 'i2',
                    }
                }
            }
            '''
            dct = form_content
            for key in keys:
                if not isinstance(dct.get(key), dict):
                    dct[key] = {}
                if key==keys[-1]:
                    if only_keep_last_value and isinstance(v, list):
                        v = v[-1]
                    dct[key] = v
                    break
                dct = dct[key]
                
        logger.debug('parsed_params=%r' % form_content)
        return form_content
