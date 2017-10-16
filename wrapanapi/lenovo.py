# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
from base import WrapanapiAPIBase


class LenovoSystem(WrapanapiAPIBase):
    """Client to Lenovo API

    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.

    """
    _api = None

    _stats_available = {
        'num_server': lambda self: len(self.list_server()),
    }

    def __init__(self, hostname, username, password, **kwargs):
        super(LenovoSystem, self).__init__(kwargs)
        self.hostname = hostname
        self.username = username
        self.password = password

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
        # This isn't the best place for this, but this class doesn't know when it is no longer in
        # use, and we need to do some sort of disconnect based on the pyVmomi documentation.
        raise NotImplementedError

    @property
    def version(self):
        """The product version"""
        raise NotImplementedError

    def list_server(self):
        raise NotImplementedError
