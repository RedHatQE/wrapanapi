# coding: utf-8
"""Backend management system classes

Used to communicate with providers without using CFME facilities
"""
import json

import requests
from requests.exceptions import Timeout

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
        'num_server': lambda self: len(self.list_servers()),
        # 'num_server_with_host': lambda self: len(self.list_servers_with_host()),
    }
    POWERED_ON = 8
    POWERED_OFF = 5
    STANDBY = 18
    HEALTH_VALID = ("normal", "non-critical")
    HEALTH_WARNING = ("warning")
    HEALTH_CRITICAL = ("critical", "minor-failure", "major-failure", "non-recoverable", "fatal")

    def __init__(self, hostname, username, password, protocol="https", port=443, **kwargs):
        self.auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, port)
        self._servers_list = None
        self.kwargs = kwargs

    def __del__(self):
        """Disconnect from the API when the object is deleted"""
        # This isn't the best place for this, but this class doesn't know when it is no longer in
        # use, and we need to do some sort of disconnect based on the pyVmomi documentation.

    def _service_instance(self, path):
        """An instance of the service"""
        try:
            response = requests.get(self.url + path, auth=self.auth, verify=False)
            return json.loads(response.content)
        except Timeout:
            return None

    @property
    def version(self):
        """The product version"""
        response = self._service_instance("aicc")
        return response['appliance']['version']

    def list_servers(self):
        response = self._service_instance("cabinet?status=includestandalone")
        cabinets = response['cabinetList'][0]
        nodes_list = cabinets['nodeList']

        inventory = [node['itemInventory'] for node in nodes_list] if len(nodes_list) > 0 else []

        nodes_from_chassis = []
        if len(cabinets['chassisList']) > 0:
            chassis_list = cabinets['chassisList'][0]
            nodes_from_chassis = [node for node in chassis_list['itemInventory']['nodes']
                            if node['type'] != 'SCU']

        inventory.extend(nodes_from_chassis)

        self._servers_list = inventory
        return inventory

    def get_server(self, server_name):
        if not self._servers_list:
            self.list_servers()

        try:
            for node in self._servers_list:
                if node['name'] == server_name:
                    return node
        except AttributeError:
            return None
