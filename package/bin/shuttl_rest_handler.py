import splunk
import os

BIN_DIR = os.path.abspath(os.path.dirname(__file__))
APP_DIR = os.path.abspath(os.path.join(BIN_DIR, '..'))

class RendersPort(splunk.rest.BaseRestHandler):

    def handle_GET(self):
        response = self.response
        request = self.request
        form = request['query']
        #self.settings = settings = splunk.clilib.cli_common.getConfStanza('pdf_server', 'settings')
        response.write('hello interwebz')

