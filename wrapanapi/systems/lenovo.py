# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import json

import requests
from requests.exceptions import Timeout

from wrapanapi.systems.base import System


class LenovoSystem(System):
    """Client to Lenovo API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
    """
    _api = None

    _server_stats_available = {
        'num_server': lambda self, _: len(self.list_servers()),
        'cores_capacity': lambda self, requester: self.get_server_cores(requester.name),
        'memory_capacity': lambda self, requester: self.get_server_memory(requester.name),
        'num_firmwares': lambda self, requester: len(self.get_server_firmwares(requester.name)),
        'num_network_devices': lambda self,
        requester: len(self.get_network_devices(requester.name)),
        'num_storage_devices': lambda self,
        requester: len(self.get_storage_devices(requester.name)),
    }
    _server_inventory_available = {
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
        super(LenovoSystem, self).__init__(**kwargs)
        self.port = port or kwargs.get('api_port', 443)
        self.auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, self.port)
        self._servers_list = None
        self.kwargs = kwargs

    @property
    def _identifying_attrs(self):
        return {'url': self.url}

    def info(self):
        return 'LenovoSystem url={}'.format(self.url)

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
        inventory = []

        # Collect the nodes associated with a cabinet or chassis
        response = self._service_instance("cabinet?status=includestandalone")
        for cabinet in response['cabinetList']:
            cabinet_nodes = cabinet['nodeList']
            inventory.extend([node['itemInventory'] for node in cabinet_nodes])

            for chassis in cabinet['chassisList']:
                chassis_nodes = chassis['itemInventory']['nodes']
                inventory.extend([node for node in chassis_nodes if node['type'] != 'SCU'])

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

    def get_server_ipv4_address(self, server_name):
        server = self.get_server(server_name)
        return server['ipv4Addresses']

    def get_server_ipv6_address(self, server_name):
        server = self.get_server(server_name)
        return server['ipv6Addresses']

    def get_server_mac_address(self, server_name):
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

        # Convert it to bytes, so it matches the value in the UI
        return (1024 * total_memory)

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

    def server_stats(self, *requested_stats, **kwargs):
        # Get the requester which represents the class of this method's caller
        requester = kwargs.get('requester')

        # Retrieve and return the stats
        requested_stats = requested_stats or self._stats_available

        return {stat: self._server_stats_available[stat](self, requester)
                for stat in requested_stats}

    def server_inventory(self, *requested_items, **kwargs):
        # Get the requester which represents the class of this method's caller
        requester = kwargs.get('requester')

        # Retrieve and return the inventory
        requested_items = requested_items or self._server_inventory_available
        return {item: self._server_inventory_available[item](self, requester)
                for item in requested_items}

    def get_network_devices(self, server_name):
        addin_cards = self.get_addin_cards(server_name) or []
        pci_devices = self.get_pci_devices(server_name) or []
        network_devices = []

        for addin_card in addin_cards:
            if (LenovoSystem.is_network_device(addin_card) and not
                    LenovoSystem.is_device_in_list(addin_card, network_devices)):
                network_devices.append(addin_card)

        for pci_device in pci_devices:
            if (LenovoSystem.is_network_device(pci_device) and not
                    LenovoSystem.is_device_in_list(pci_device, network_devices)):
                network_devices.append(pci_device)

        return network_devices

    def get_storage_devices(self, server_name):
        addin_cards = self.get_addin_cards(server_name) or []
        pci_devices = self.get_pci_devices(server_name) or []
        storage_devices = []

        for addin_card in addin_cards:
            if (LenovoSystem.is_storage_device(addin_card) and not
                    LenovoSystem.is_device_in_list(addin_card, storage_devices)):
                storage_devices.append(addin_card)

        for pci_device in pci_devices:
            if (LenovoSystem.is_storage_device(pci_device) and not
                    LenovoSystem.is_device_in_list(pci_device, storage_devices)):
                storage_devices.append(pci_device)

        return storage_devices

    @staticmethod
    def is_device_in_list(device, device_list):
        device_id = LenovoSystem.get_device_unique_id(device)

        for d in device_list:
            if device_id == LenovoSystem.get_device_unique_id(d):
                return True

        return False

    @staticmethod
    def is_network_device(device):
        # The device name is stored in either the "productName" field or the "name" field.
        device_name = device.get("productName") or device.get("name")
        device_name = device_name.lower()

        # We expect that supported network devices will have a class of "network controller" or
        # "nic" or "ethernet" contained in the device name.
        return (device.get("class").lower() == "network controller" or
                "nic" in device_name or
                "ethernet" in device_name)

    @staticmethod
    def is_storage_device(device):
        # The device name is stored in either the "productName" field or the "name" field.
        device_name = device.get("productName") or device.get("name")
        device_name = device_name.lower()

        # We expect that supported storage devices will have a class of "mass storage controller"
        # or "serveraid" or "sd media raid" contained in the device name.
        return (device.get("class").lower() == "mass storage controller" or
                "serveraid" in device_name or
                "sd media raid" in device_name)

    def get_addin_cards(self, server_name):
        server = self.get_server(server_name)

        return server.get("addinCards")

    def get_pci_devices(self, server_name):
        server = self.get_server(server_name)

        return server.get("pciDevices")

    @staticmethod
    def get_device_unique_id(device):
        # The ID used to uniquely identify each device is the UUID of the device
        # if it has one or the concatenation of the PCI bus number and PCI device number.
        unique_id = (device.get("uuid") or
                     "{}{}".format(device.get("pciBusNumber"), device.get("pciDeviceNumber")))

        return unique_id

    def disconnect(self):
        self.logger.info("LenovoSystem disconnected")
