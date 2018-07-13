# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

from wrapanapi.systems.base import System


class RedfishSystem(System):
    """Client to Redfish API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
    """

    _stats_available = {
        'num_server': lambda self: 1,
    }

    def __init__(self, hostname, username, password, protocol="https", api_port=443, **kwargs):
        super(RedfishSystem, self).__init__(kwargs)
        self.api_port = api_port
        self.auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, self.api_port)
        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {'url': self.url}

    def info(self):
        return 'RedfishSystem url={}'.format(self.url)

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
