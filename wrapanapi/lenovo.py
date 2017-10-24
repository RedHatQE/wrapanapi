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
        'num_server_with_host': lambda self: len(self.list_server_with_host()),
    }
    POWERED_ON = 8
    POWERED_OFF = 5
    STANDBY = 18
    HEALTH_VALID = ("normal", "non-critical")
    HEALTH_WARNING = ("warning")
    HEALTH_CRITICAL = ("critical", "minor-failure", "major-failure", "non-recoverable", "fatal")

    def __init__(self, hostname, username, password, protocol="https", port=443, **kwargs):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.protocol = protocol

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
        # This isn't the best place for this, but this class doesn't know when it is no longer in
        # use, and we need to do some sort of disconnect based on the pyVmomi documentation.

    def __service_instance(self, path):
        """An instance of the service"""
        try:
            uri = urlunparse((self.protocol, self.hostname, path, "", "", ""))
            response = requests.get(uri, auth=(self.username, self.password), verify=False)
            return json.loads(response.content)
        except Timeout:
            return None

    @property
    def version(self):
        """The product version"""
        response = self.__service_instance("/aicc")
        return response['appliance']['version']

    def list_servers(self):
        response = self.__service_instance("/cabinet?status=includestandalone")
        cabinets = response['cabinetList'][0]
        nodes_list = cabinets['nodeList']

        inventory_list = map(lambda x: x['itemInventory'], nodes_list)
        inventory_list = filter(lambda x: x['type'] != 'SCU', inventory_list)

        chassis_nodes_list = cabinets['chassisList']
        nodes_from_chassis_list = map(lambda x: x['itemInventory']['nodes'], chassis_nodes_list)

        inventory_list.extend(nodes_from_chassis_list)
        return inventory_list

    def get_server(self, server_name):
        try:
            servers = self.list_servers()
            for node in servers:
                if node['name'] == server_name:
                    return node
        except AttributeError:
            return None
