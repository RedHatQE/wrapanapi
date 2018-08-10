# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import redfish_client
import json

from wrapanapi.systems.base import System


class RedfishSystem(System):
    """Client to Redfish API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
        security_protocol: The security protocol to be used for connecting with
            the API. Expected values: 'Non-SSL', 'SSL', 'SSL without validation'
    """

    _stats_available = {
        'num_server': lambda self: self.num_servers(),
    }

    def __init__(self, hostname, username, password, security_protocol, api_port=443, **kwargs):
        super(RedfishSystem, self).__init__(**kwargs)
        protocol = 'http' if security_protocol == 'Non-SSL' else 'https'
        self.url = '{}://{}:{}/'.format(protocol, hostname, api_port)
        self.kwargs = kwargs
        self.api_client = redfish_client.connect(self.url, username, password)

    @property
    def _identifying_attrs(self):
        return {'url': self.url}

    def info(self):
        return 'RedfishSystem url={}'.format(self.url)

    def __del__(self):
        """Disconnect from the API when the object is deleted"""

    def num_servers(self):
        return len(self.api_client.Systems.Members)
