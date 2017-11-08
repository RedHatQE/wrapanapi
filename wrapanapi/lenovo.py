# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
import requests
import json
from base import WrapanapiAPIBase
from urlparse import urlunparse
from requests.exceptions import Timeout


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

    def __init__(self, hostname, username, password, protocol="https", port=443, **kwargs):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.protocol = protocol
        self._content = None

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
        # This isn't the best place for this, but this class doesn't know when it is no longer in
        # use, and we need to do some sort of disconnect based on the pyVmomi documentation.
        self.port = self.port

    def __service_instance(self, path):
        """An instance of the service"""
        try:
            uri = urlunparse((self.protocol, self.hostname, path, "", "", ""))
            response = requests.get(uri, auth=(self.username, self.password), verify=False)
            self._content = json.loads(response.content)
            return self._content
        except Timeout:
            return None

    @property
    def version(self):
        """The product version"""
        response = self.__service_instance("/aicc")
        versionstr = response['appliance']['version']
        return versionstr

    def list_servers(self):
        response = self.__service_instance("/cabinet?status=includestandalone")
        # TODO this only parses the first list of nodes in the cabinet. Need to abstract this method
        # cabinets = response['cabinetList']
        # map(lambda x: x['nodeList'], cabinets)
        cabinets = response['cabinetList'][0]
        nodelist = cabinets['nodeList']
        inventorylist = map(lambda x: x['itemInventory'], nodelist)
        inventorylist = filter(lambda x: x['type'] != 'SCU', inventorylist)
        return inventorylist
