# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import json

import requests
from requests.exceptions import Timeout

from .base import WrapanapiAPIBase


class LenovoSystem(WrapanapiAPIBase):
    """Client to Lenovo API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
    """
    _api = None

    _stats_available = {
        'num_server': lambda self, requester: len(self.list_servers(requester)),
        'cores_capacity': lambda self, requester: self.get_server_cores(requester.name),
        'memory_capacity': lambda self, requester: self.get_server_memory(requester.name),
        'num_firmwares': lambda self, requester: len(self.get_server_firmwares(requester.name)),
    }
    _inventory_available = {
        'hostname': lambda self, requester: self.get_server_hostname(requester.name),
        'ipv4_address': lambda self, requester: self.get_server_ipv4_address(requester.name),
        'ipv6_address': lambda self, requester: self.get_server_ipv6_address(requester.name),
        'mac_address': lambda self, requester: self.get_server_mac_address(requester.name),
        'power_state': lambda self, requester: self.get_server_power_status(requester.name),
        'health_state': lambda self, requester: self.get_server_health_state(requester.name),
        'manufacturer': lambda self, requester: self.get_server_manufacturer(requester.name),
        'model': lambda self, requester: self.get_server_model(requester.name),
        'machine_type': lambda self, requester: self.get_server_machine_type(requester.name),
        'serial_number': lambda self, requester: self.get_server_serial_number(requester.name),
        'description': lambda self, requester: self.get_server_description(requester.name),
        'product_name': lambda self, requester: self.get_server_product_name(requester.name),
        'uuid': lambda self, requester: self.get_server_uuid(requester.name),
        'field_replaceable_unit': lambda self, requester: self.get_server_fru(requester.name),
    }
    POWERED_ON = 8
    POWERED_OFF = 5
    STANDBY = 18
    HEALTH_VALID = ("normal", "non-critical")
    HEALTH_WARNING = ("warning")
    HEALTH_CRITICAL = ("critical", "minor-failure", "major-failure", "non-recoverable", "fatal")

    def __init__(self, hostname, username, password, protocol="https", port=None, **kwargs):
        super(LenovoSystem, self).__init__(kwargs)
        self.port = port or kwargs.get('api_port', 443)
        self.auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, self.port)
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

    def _service_put(self, path, request):
        """An instance of the service"""
        try:
            response = requests.put(self.url + path, data=json.dumps(request), auth=self.auth,
                                    verify=False)
            return response
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

        inventory = [node['itemInventory'] for node in nodes_list]

        nodes_from_chassis = []
        if len(cabinets['chassisList']) > 0:
            chassis_list = cabinets['chassisList'][0]
            nodes_from_chassis = [node for node in chassis_list['itemInventory']['nodes']
                                  if node['type'] != 'SCU']

        inventory.extend(nodes_from_chassis)

        self._servers_list = inventory
        return inventory

    def change_node_power_status(self, server, request):
        url = "nodes/" + str(server['uuid'])
        payload = {'powerState': request}
        response = self._service_put(url, payload)

        return response

    def change_led_status(self, server, name, state):
        url = "nodes/" + str(server['uuid'])
        payload = {'leds': [{'name': name, 'state': state}]}
        response = self._service_put(url, payload)

        return response

    def get_server(self, server_name):
        if not self._servers_list:
            self.list_servers()

        try:
            for node in self._servers_list:
                if node['name'] == server_name:
                    return node
        except AttributeError:
            return None

    def get_led(self, server_name):
        try:
            server = self.get_server(server_name)
            leds = server['leds']
            for led in leds:
                if led['name'] == 'Identify' or led['name'] == 'Identification':
                    return led
        except AttributeError:
            return None

    def get_server_hostname(self, server_name):
        server = self.get_server(server_name)

        return str(server['hostname'])

    def get_server_ipv4_address(self, server_name, timeout=600):
        server = self.get_server(server_name)

        return server['ipv4Addresses']

    def get_server_ipv6_address(self, server_name, timeout=600):
        server = self.get_server(server_name)

        return server['ipv6Addresses']

    def get_server_mac_address(self, server_name, timeout=600):
        server = self.get_server(server_name)

        return server['macAddress']

    def get_server_power_status(self, server_name):
        server = self.get_server(server_name)

        if server['powerStatus'] == self.POWERED_ON:
            return "on"
        elif server['powerStatus'] == self.POWERED_OFF:
            return "off"
        elif server['powerStatus'] == self.STANDBY:
            return "Standby"
        else:
            return "Unknown"

    def get_server_health_state(self, server_name):
        server = self.get_server(server_name)

        if str(server['cmmHealthState'].lower()) in self.HEALTH_VALID:
            return "Valid"
        elif str(server['cmmHealthState'].lower()) in self.HEALTH_WARNING:
            return "Warning"
        elif str(server['cmmHealthState'].lower()) in self.HEALTH_CRITICAL:
            return "Critical"
        else:
            return "Unknow"

    def is_server_running(self, server_name):
        server = self.get_server(server_name)

        return server['powerStatus'] == self.POWERED_ON

    def is_server_stopped(self, server_name):
        server = self.get_server(server_name)

        return server['powerStatus'] == self.POWERED_OFF

    def is_server_standby(self, server_name):
        server = self.get_server(server_name)

        return server['powerStatus'] == self.STANDBY

    def is_server_valid(self, server_name):
        server = self.get_server(server_name)

        return str(server['cmmHealthState'].lower()) in self.HEALTH_VALID

    def is_server_warning(self, server_name):
        server = self.get_server(server_name)

        return str(server['cmmHealthState'].lower()) in self.HEALTH_WARNING

    def is_server_critical(self, server_name):
        server = self.get_server(server_name)

        return str(server['cmmHealthState'].lower()) in self.HEALTH_CRITICAL

    def is_server_led_on(self, server_name):
        led = self.get_led(server_name)

        return led['state'] == 'On'

    def is_server_led_off(self, server_name):
        led = self.get_led(server_name)

        return led['state'] == 'Off'

    def is_server_led_blinking(self, server_name):
        led = self.get_led(server_name)

        return led['state'] == 'Blinking'

    def get_server_cores(self, server_name):
        server = self.get_server(server_name)
        processors = server['processors']
        cores = sum([processor['cores'] for processor in processors])

        return cores

    def get_server_memory(self, server_name):
        server = self.get_server(server_name)
        memorys = server['memoryModules']
        total_memory = sum([memory['capacity'] for memory in memorys])

        return total_memory

    def get_server_manufacturer(self, server_name):
        server = self.get_server(server_name)

        return str(server['manufacturer'])

    def get_server_model(self, server_name):
        server = self.get_server(server_name)

        return str(server['model'])

    def get_server_machine_type(self, server_name):
        server = self.get_server(server_name)

        return str(server['machineType'])

    def get_server_serial_number(self, server_name):
        server = self.get_server(server_name)

        return str(server['serialNumber'])

    def get_server_description(self, server_name):
        server = self.get_server(server_name)

        return str(server['description'])

    def get_server_product_name(self, server_name):
        return self.get_server(server_name)['productName']

    def get_server_uuid(self, server_name):
        return self.get_server(server_name)['uuid']

    def get_server_fru(self, server_name):
        return self.get_server(server_name)['FRU']

    def get_server_firmwares(self, server_name):
        return self.get_server(server_name)['firmware']

    def set_power_on_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'powerOn')

        return "Power state action has been sent, status:" + str(response.status_code)

    def set_power_off_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'powerOffSoftGraceful')

        return "Power state action has been sent, status:" + str(response.status_code)

    def set_power_off_immediately_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'powerOff')

        return "Power state action has been sent, status:" + str(response.status_code)

    def set_restart_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'powerOffSoftGraceful')

        return "Restart state action has been sent, status:" + str(response.status_code)

    def set_restart_immediately_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'powerCycleSoft')

        return "Restart state action has been sent, status:" + str(response.status_code)

    def set_restart_setup_system_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'bootToF1')

        return "Restart state action has been sent, status:" + str(response.status_code)

    def set_restart_controller_server(self, server_name):
        server = self.get_server(server_name)
        response = self.change_node_power_status(server, 'restart')

        return "Restart state action has been sent, status:" + str(response.status_code)

    def set_server_led_on(self, server_name):
        server = self.get_server(server_name)
        led = self.get_led(server_name)
        response = self.change_led_status(server, led['name'], 'On')

        return "LED state action has been sent, status:" + str(response.status_code)

    def set_server_led_off(self, server_name):
        server = self.get_server(server_name)
        led = self.get_led(server_name)
        response = self.change_led_status(server, led['name'], 'Off')

        return "LED state action has been sent, status:" + str(response.status_code)

    def set_server_led_blinking(self, server_name):
        server = self.get_server(server_name)
        led = self.get_led(server_name)
        response = self.change_led_status(server, led['name'], 'Blinking')

        return "LED state action has been sent, status:" + str(response.status_code)

    def stats(self, *requested_stats, **kwargs):
        # Get the requester which represents the class of this method's caller
        requester = kwargs.get('requester')

        # Retrieve and return the stats
        requested_stats = requested_stats or self._stats_available
        return {stat: self._stats_available[stat](self, requester) for stat in requested_stats}

    def inventory(self, *requested_items, **kwargs):
        # Get the requester which represents the class of this method's caller
        requester = kwargs.get('requester')

        # Retrieve and return the inventory
        requested_items = requested_items or self._inventory_available
        return {item: self._inventory_available[item](self, requester) for item in requested_items}

    def disconnect(self):
        self.logger.info("LenovoSystem disconnected")
