import splunk
import os
import json
from xml.dom import minidom

BIN_DIR = os.path.abspath(os.path.dirname(__file__))
APP_DIR = os.path.abspath(os.path.join(BIN_DIR, '..'))
CONF_DIR = os.path.abspath(os.path.join(APP_DIR, 'conf'))
SHUTTL_CONF = os.path.abspath(os.path.join(CONF_DIR, 'server.xml'))

class RendersPort(splunk.rest.BaseRestHandler):

    def handle_GET(self):
        response = self.response
        response.write(json.dumps({'shuttl_port' : getShuttlPort() }, indent=2))

def getShuttlPort():
    xmldoc = minidom.parse(SHUTTL_CONF)
    httpPortNode = xmldoc.getElementsByTagName('httpPort')[0]
    return httpPortNode.childNodes[0].nodeValue

