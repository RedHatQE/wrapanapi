# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import redfish_client

from wrapanapi.entities import PhysicalContainer, Server, ServerState
from wrapanapi.entities.base import Entity
from wrapanapi.exceptions import InvalidValueException, ItemNotFound
from wrapanapi.systems.base import System


class RedfishItemNotFound(ItemNotFound):
    """Raised if a Redfish item is not found."""
    def __init__(self, name, item_type, response):
        super(RedfishItemNotFound, self).__init__(name, item_type)
        self.response = response

    def __str__(self):
        return 'Could not find a {} named {}. Response:\n{}'.format(self.item_type, self.name,
            self.response)


class RedfishResource(Entity):
    """Class representing a generic Redfish resource such as Server or Chassis. """

    def __init__(self, system, raw=None, **kwargs):
        """
        Constructor for RedfishResource.

        Args:
            system: RedfishSystem instance
            raw: the root resource in the Redfish API
            odata_id: (optional) the @odata.id reference of this instance
        """
        self._odata_id = raw['@odata.id'] if raw else kwargs.get('odata_id')
        if not self._odata_id:
            raise ValueError("missing required kwargs: 'odata_id'")

        super(RedfishResource, self).__init__(system, raw, **kwargs)

    @property
    def _identifying_attrs(self):
        """
        Return the list of attributes that make this instance uniquely identifiable.

        These attributes identify the instance without needing to query the API
        for updated data.
        """
        return {'odata_id': self._odata_id}

    def refresh(self):
        """
        Re-pull data for this entity using the system's API and update this instance's attributes.

        This method should be called any time the most up-to-date info needs to be
        returned

        This method should re-set self.raw with fresh data for this entity

        Returns:
            New value of self.raw
        """
        self.raw._cache = {}

    @property
    def name(self):
        """Return name from most recent raw data."""
        return self.raw.Id

    @property
    def description(self):
        """Return description from most recent raw data."""
        return self.raw.Description

    def uuid(self):
        """Return uuid from most recent raw data."""
        return self.raw.Id


class RedfishServer(Server, RedfishResource):
    state_map = {
        'On': ServerState.ON,
        'Off': ServerState.OFF,
        'PoweringOn': ServerState.POWERING_ON,
        'PoweringOff': ServerState.POWERING_OFF,
    }

    @property
    def server_cores(self):
        """Return the number of cores on this server."""
        return sum(int(p.TotalCores) for p in self.raw.Processors.Members)

    @property
    def server_memory(self):
        """Return the amount of memory on the server, in MiB."""
        return self.raw.MemorySummary.TotalSystemMemoryGiB * 1024

    @property
    def state(self):
        """Retrieve the current power status of the physical server."""
        return self.raw.PowerState

    def _get_state(self):
        """
        Return ServerState object representing the server's current state.

        The caller should call self.refresh() first to get the latest status
        from the API.
        """
        return self._api_state_to_serverstate(self.state)


class RedfishChassis(PhysicalContainer, RedfishResource):
    """API handler for this instance of the physical container."""

    @property
    def chassis_type(self):
        """Retrieve the type of this chassis."""
        return self.raw.ChassisType

    @property
    def led_state(self):
        """Retrieve the status of the identifying LED on this chassis."""
        return self.raw.raw.get("IndicatorLED", "")

    @property
    def num_servers(self):
        """Retrieve the number of physical servers within this chassis."""
        return len(self.raw.Links.raw.get("ComputerSystems", []))


class RedfishSystem(System):
    """Client to Redfish API.

    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
        security_protocol: The security protocol to be used for connecting with
            the API. Expected values: 'Non-SSL', 'SSL', 'SSL without validation'
    """

    # statistics for the provider
    _stats_available = {
        'num_server': lambda system: system.num_servers,
        'num_chassis': lambda system: system.num_chassis,
        'num_racks': lambda system: system.num_racks,
    }

    # statistics for an individual server
    _server_stats_available = {
        'cores_capacity': lambda server: server.server_cores,
        'memory_capacity': lambda server: server.server_memory,
    }

    _server_inventory_available = {
        'power_state': lambda server: server.state.lower(),
    }

    # rack statistics

    _rack_stats_available = {
    }

    _rack_inventory_available = {
        'rack_name': lambda rack: rack.name,
    }

    _chassis_stats_available = {
        'num_physical_servers': lambda chassis: chassis.num_servers,
    }

    _chassis_inventory_available = {
        'chassis_name': lambda chassis: chassis.name,
        'description': lambda chassis: chassis.description,
        'identify_led_state': lambda chassis: chassis.led_state,
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

    def rack_stats(self, physical_rack, requested_stats):
        """
        Evaluate the requested rack stats at the API server.

        Returns a dictionary of stats and their respective evaluated values.

        Args:
          physical_rack: representation for the class of this method's caller
          requested_stats: the statistics to be obtained for the rack
        """
        # Retrieve and return the stats
        requested_stats = requested_stats or self._rack_stats_available

        # Get an instance of the requested Redfish rack
        redfish_rack = self.get_rack(physical_rack.ems_ref)

        return {stat: self._rack_stats_available[stat](redfish_rack)
                for stat in requested_stats}

    def rack_inventory(self, physical_rack, requested_items):
        """
        Evaluate the requested inventory item statuses at the API server.

        Returns a dictionary of items and their respective evaluated values.

        Args:
          physical_rack: representation for the class of this method's caller
          requested_items: the inventory items to be obtained for the rack
        """
        # Retrieve and return the inventory
        requested_items = requested_items or self._rack_inventory_available

        # Get an instance of the requested Redfish rack
        redfish_rack = self.get_rack(physical_rack.ems_ref)

        return {item: self._rack_inventory_available[item](redfish_rack)
                for item in requested_items}

    def chassis_stats(self, physical_chassis, requested_stats):
        """
        Evaluate the requested chassis stats at the API server.

        Returns a dictionary of stats and their respective evaluated values.

        Args:
          physical_chassis: representation for the class of this method's caller
          requested_stats: the statistics to be obtained for the chassis
        """
        # Retrieve and return the stats
        requested_stats = requested_stats or self._chassis_stats_available

        # Get an instance of the requested Redfish chassis
        redfish_chassis = self.get_chassis(physical_chassis.ems_ref)

        return {stat: self._chassis_stats_available[stat](redfish_chassis)
                for stat in requested_stats}

    def chassis_inventory(self, physical_chassis, requested_items):
        """
        Evaluate the requested inventory item statuses at the API server.

        Returns a dictionary of items and their respective evaluated values.

        Args:
          physical_chassis: representation for the class of this method's caller
          requested_items: the inventory items to be obtained for the chassis
        """
        # Retrieve and return the inventory
        requested_items = requested_items or self._chassis_inventory_available

        # Get an instance of the requested Redfish chassis
        redfish_chassis = self.get_chassis(physical_chassis.ems_ref)

        return {item: self._chassis_inventory_available[item](redfish_chassis)
                for item in requested_items}

    def find(self, resource_id):
        """
        Fetch an instance of the Redfish resource represented by resource_id.

        Args:
          resource_id: the Redfish @odata.id of the resource representing the
            resource to be retrieved

        Raises:
          RedfishItemNotFound: if the resource_id refers to a non-existing
            resource or there is an error retrieving it.
        """
        try:
            return self.api_client.find(resource_id)
        except Exception as e:
            raise RedfishItemNotFound(resource_id, "Redfish item", e.message)

    def get_server(self, resource_id):
        """
        Fetch a RedfishServer instance of the physical server representing resource_id.

        Args:
          resource_id: the Redfish @odata.id of the resource representing the
             server to be retrieved

        Raises:
          RedfishItemNotFound: if the resource_id refers to a non-existing
            resource or there is an error retrieving it.
        """
        return RedfishServer(self, raw=self.find(resource_id))

    def get_chassis(self, resource_id, *required_types):
        """
        Fetch a RedfishChassis instance of the physical chassis representing resource_id.

        In Redfish, a Chassis may be of specific type such as Rack, Sled, Block
        or any other type. Use the required_types optional parameter to filter
        by type, or use any of the specific getters such as get_rack.

        Args:
          resource_id: the Redfish @odata.id of the resource representing the
             chassis to be retrieved
          required_types: optional list of one or more strings. If present, the
             retrieved resource's ChassisType property value needs to be equal
             to one of the strings in the list
        Raises:
           InvalidValueException if the resource_id represents a Chassis that is
              not of any of the required types
          RedfishItemNotFound: if the resource_id refers to a non-existing
            resource or there is an error retrieving it.
        """
        chassis = RedfishChassis(self, raw=self.find(resource_id))

        if required_types and chassis.raw.ChassisType not in required_types:
            raise InvalidValueException(
                "This chassis is of wrong type {}".format(chassis.raw.ChassisType))

        return chassis

    def get_rack(self, resource_id):
        """
        Fetch a RedfishChassis instance of the physical rack representing resource_id.

        Args:
          resource_id: the Redfish @odata.id of the resource representing the
             chassis to be retrieved
        Raises:
           InvalidValueException if the resource_id represents a Chassis that is
             not a rack
        """
        return self.get_chassis(resource_id, "Rack")

    @property
    def num_servers(self):
        """Return the number of servers discovered by the provider."""
        return len(self.api_client.Systems.Members)

    @property
    def num_chassis(self):
        """Return the count of Physical Chassis discovered by the provider."""
        return len([chassis for chassis in self.api_client.Chassis.Members
            if chassis.ChassisType != "Rack"])

    @property
    def num_racks(self):
        """Return the number of Physical Racks discovered by the provider."""
        return len([rack for rack in self.api_client.Chassis.Members
            if rack.ChassisType == "Rack"])
