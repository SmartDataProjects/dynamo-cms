import logging

from dynamo.utils.interface.webservice import RESTService, GET
from dynamo.dataformat import Configuration

LOG = logging.getLogger(__name__)

class SiteDB(RESTService):

    _url_base = ''
    _num_attempts = 1
    
    @staticmethod
    def set_default(config):
        SiteDB._url_base = config.url_base
        SiteDB._num_attempts = config.num_attempts

    def __init__(self, config = None):
        config = Configuration(config)

        config.auth_handler = 'HTTPSCertKeyHandler'
        if 'url_base' not in config:
            config.url_base = SiteDB._url_base
        if 'num_attempts' not in config:
            config.num_attempts = SiteDB._num_attempts

        RESTService.__init__(self, config)

    def make_request(self, resource = '', options = [], method = GET, format = 'url', retry_on_error = True, timeout = 0): #override
        """
        Strip the "header" and return the body JSON.
        """

        response = RESTService.make_request(self, resource, options = options, method = method, format = format, retry_on_error = retry_on_error, timeout = timeout)

        try:
            result = response['result']
        except KeyError:
            LOG.error(response)
            return

        return result
