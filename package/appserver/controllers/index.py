import logging

import splunk.rest
import splunk.bundle
import splunk.entity
from splunk.models.base import SplunkAppObjModel
from splunk.models.field import Field, BoolField

logger = logging.getLogger('splunk.models.shuttl.Index')


class Index(SplunkAppObjModel):

    resource            = 'configs/conf-indexes'
    conf_name           = 'indexes'
    owner               = 'nobody'
    namespace           = 'shuttl'
    
    description         = Field()
    is_disabled         = BoolField('disabled', is_mutable=False)
    coldToFrozenScript  = Field()
    homePath            = Field()
    coldPath            = Field()
    thawedPath          = Field()

    def _reload(self):
        return self.execute_entity_request(
            action='_reload', method='POST', default_return=False)
    
    def remove(self):
        return self.execute_entity_request(action='remove', method='DELETE')
    
    def enable(self):
        return self.execute_entity_request(action='enable')

    def disable(self):
        return self.execute_entity_request(action='disable')
    
    def execute_entity_request(self, action, method='POST', default_return=True):
        if not self.action_links:
            return False
        for item in self.action_links:
            if action in item: 
                response, content = splunk.rest.simpleRequest(item[1], 
                                         method=method)
                if response.status == 200:
                    return True
        return default_return 
    
    @classmethod
    def create_template(cls, name, namespace=None, owner=None, sessionKey=None):
        resource = cls.resource
        if not namespace:
            namespace = cls.namespace
        if not owner:
            owner = cls.owner
        
        # Create the index using properties endpoint.
        conf = Index.get_index_conf(cls.conf_name, cls.namespace, cls.owner, sessionKey=sessionKey)
        conf[name]['_SomeValueToInstantiateANewStanza_'] = ''  # Hack because it's impossible to propagate the value to the conf-file otherwise.
        
        if conf[name]: logger.info('Created new index with name=%r, owner=%r'
            ' namespace=%r' % (name, owner, namespace))
        else: logger.warn("Couldn't create new index with name=%s" % name)
        
        entity = splunk.entity.getEntity(resource, name, namespace=namespace, owner=owner)
        return cls(namespace, owner, name, entity=entity)
    
    @classmethod
    def get_index_conf(cls, conf_name, namespace, owner, sessionKey=None, overwriteStanzas=False, hostPath=None):
        return splunk.bundle.getConf(conf_name, namespace=namespace, 
            owner=owner, sessionKey=sessionKey, 
            overwriteStanzas=overwriteStanzas, hostPath=hostPath)

    def save(self):
        logger.info('(save) conf_name=%r stanza=%r namespace=%r owner=%r' % 
                    (self.conf_name, self.name, self.namespace, self.owner))
        conf = Index.get_index_conf(self.conf_name, self.namespace, self.owner)
        # update properties if they have changed.
        for attr, attr_value in self.__class__.__dict__.iteritems():
            if isinstance(attr_value, Field):
                
                field = attr_value
                stanza_name = self.name
                field_name = field.get_api_name(attr)
                field_value = field.to_apidata(getattr(self, attr, None))
                logger.debug('(save) Got field, name=%r api_name=%r value=%r' % 
                            (attr, field_name, field_value))
                
                if self.handle_special_field(field_name):
                    continue
                
                if stanza_name and field_name and field.get_is_mutable():
                    # Create stanza if it doesn't exist.
                    cur_value = conf[stanza_name].get(field_name, None)
                    if not cur_value and field_value!=None and cur_value!=field_value:
                        conf[stanza_name][field_name] = field_value
                else:
                    logger.debug('(save) Skipped field in stanza=%r with name=%r, value=%r' % (stanza_name, field_name, field_value))
        
        # TODO: You cannot reuse this object! without a refill. 
            # get entity
            # self._fill_entity(self.entity)
            
        return True
    
    def handle_special_field(self, field_name, field=None):
        if field_name in ['disabled', 'is_disabled']:
            # Use endpoints to enable/disabled.
            return True
        return False
