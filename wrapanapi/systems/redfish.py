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

    def server_stats(self, physical_server, requested_stats, **kwargs):
        """
        Evaluate the requested server stats at the API server.

        Returns a dictionary of stats and their respective evaluated values.

        Args:
          physical_server: representation for the class of this method's caller
          requested_stats: the statistics to be obtained
        """
        # Retrieve and return the stats
        requested_stats = requested_stats or self._stats_available

        # Get an instance of the requested Redfish server
        redfish_server = self.get_server(physical_server.ems_ref)

        return {stat: self._server_stats_available[stat](redfish_server)
                for stat in requested_stats}

    def server_inventory(self, physical_server, requested_items, **kwargs):
        """
        Evaluate the requested inventory item statuses at the API server.

        Returns a dictionary of items and their respective evaluated values.

        Args:
          physical_server: representation for the class of this method's caller
          requested_items: the inventory items to be obtained for the server
        """
        # Retrieve and return the inventory
        requested_items = requested_items or self._server_inventory_available

        # Get an instance of the requested Redfish server
        redfish_server = self.get_server(physical_server.ems_ref)

        return {item: self._server_inventory_available[item](redfish_server)
                for item in requested_items}

    def get_server(self, resource_id):
        """
        Fetch a RedfishServer instance of the physical server representing resource_id.

        Args:
          resource_id: the Redfish @odata.id of the resource representing the
             server to be retrieved
        """
        return RedfishServer(self, raw=self.api_client.find(resource_id))

    @property
    def num_servers(self):
        return len(self.api_client.Systems.Members)
