# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from base import WrapanapiAPIBase


class NuageSystem(WrapanapiAPIBase):
    """Client to Nuage API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
    """

    _stats_available = {
        'num_security_group': lambda self: len(self.list_security_groups()),
    }

    def __init__(self, hostname, username, password, protocol="https", port=443, **kwargs):
        super(NuageSystem, self).__init__(kwargs)
        self.auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, port)

    def list_security_groups(self):
        pass
