from splunk.models.base import SplunkAppObjModel
from splunk.models.field import Field, IntField

class Model(SplunkAppObjModel):

    resource    = 'saved/eventtypes'
    search      = Field() 
    priority    = IntField()
    description = Field()

    def _reload(self):
        if not self.action_links:
            return False
        for item in self.action_links:
            if '_reload' in item: 
                response, content = rest.simpleRequest(item[1], 
                                         method='POST')
                if response.status == 200:
                    return True
        return False
 
    def enable(self):
        if not self.action_links:
            return False
        for item in self.action_links:
            if 'enable' in item: 
                response, content = rest.simpleRequest(item[1], 
                                         method='POST')
                if response.status == 200:
                    return True
        return True 
 
    def disable(self):
        if not self.action_links:
            return False
        for item in self.action_links:
            if 'disable' in item: 
                response, content = rest.simpleRequest(item[1], 
                                         method='POST')
                if response.status == 200:
                    return True
        return True 
    
