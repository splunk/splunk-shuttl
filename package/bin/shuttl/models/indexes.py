import splunk.rest as rest
from splunk.models.base import SplunkAppObjModel
from splunk.models.field import Field, BoolField, FloatField

'''
Provides object mapping for index objects
'''

class Index(SplunkAppObjModel):
    
    resource = 'data/indexes'

    def enable(self):
        output = False
        if self.action_links:
            for item in self.action_links:
                if 'enable' in item: 
                    response, content = rest.simpleRequest(item[1], 
                                             method='POST')
                    if response.status == 200:
                        output = True
        return output 
 
    def disable(self):
        output = False
        if self.action_links:
            for item in self.action_links:
                if 'enable' in item: 
                    response, content = rest.simpleRequest(item[1], 
                                             method='POST')
                    if response.status == 200:
                        output = True
        return output 
