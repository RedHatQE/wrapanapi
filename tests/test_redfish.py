# coding: utf-8
# Unit tests for the Redfish API
import pytest
from unittest import mock

from redfish_client.root import Root
from wrapanapi.exceptions import InvalidValueException
from wrapanapi.entities import ServerState
from wrapanapi.systems import RedfishSystem
from wrapanapi.systems.redfish import RedfishChassis, RedfishRack, RedfishServer


@mock.patch("redfish_client.connect")
class TestRedfishSystemSetup:
    def test_connect_non_ssl(self, mock_connector):
        RedfishSystem("dummy", "user", "pass", "Non-SSL", "8000")
        mock_connector.assert_called_with("http://dummy:8000/", "user", "pass")

    def test_connect_ssl(self, mock_connector):
        RedfishSystem("dummy", "user", "pass", "SSL", "443")
        mock_connector.assert_called_with("https://dummy:443/", "user", "pass")


class RedfishTestCase():
    def mock_redfish_system(self, mock_connector, data):
        def mock_get(url):
            res = mock.Mock()
            res.status = 200
            res.json = data.get(url, {})
            return res

        mock_connector.get.side_effect = mock_get
        api_client = Root(mock_connector)
        api_client._content = data.get("/redfish/v1", None)

        rf = RedfishSystem("dummy", "user", "pass", "Non-SSL", "8000",
            api_client=api_client)
        return rf

    def setup_method(self, method):
        self.patcher = mock.patch("redfish_client.Connector")
        ConnectorMockClass = self.patcher.start()
        self.mock_connector = ConnectorMockClass()

    def teardown_method(self, method):
        self.patcher.stop()


class TestRedfishSystem(RedfishTestCase):
    def test_find_resource(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/BlankResource": {
                "@odata.id": "id"
            }
        })
        resource = rf.find("/redfish/v1/BlankResource")
        assert resource.raw == {"@odata.id": "id"}

    def test_redfish_system(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/BlankResource": {
                "@odata.id": "id"
            }
        })
        assert rf._identifying_attrs == {"url": "http://dummy:8000/"}
        assert rf.info() == "RedfishSystem url=http://dummy:8000/"

    def test_num_servers(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1": {
                "Systems": {
                    "@odata.id": "/redfish/v1/Systems"
                }
            },
            "/redfish/v1/Systems": {
                "@odata.id": "/redfish/v1/Systems/",
                "Members@odata.count": 4,
                "@odata.context":
                    "/redfish/v1/$metadata#ComputerSystemCollection.ComputerSystemCollection",
                "Members": [
                    {"@odata.id": "/redfish/v1/Systems/1"},
                    {"@odata.id": "/redfish/v1/Systems/2"},
                    {"@odata.id": "/redfish/v1/Systems/3"},
                    {"@odata.id": "/redfish/v1/Systems/4"},
                ],
                "@odata.type": "#ComputerSystemCollection.ComputerSystemCollection",
                "Members@odata.navigationLink": "/redfish/v1/Systems/Members",
                "@odata.etag": "W/\"e48557da1bf040a5d45d1e5aa726bf3a\"",
                "Name": "ComputerSystemCollection",
                "Description": "A Collection of ComputerSystem resource instances."}
        })
        assert rf.num_servers == 4

    def test_num_chassis(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1": {
                "Chassis": {
                    "@odata.id": "/redfish/v1/Chassis"
                }
            },
            "/redfish/v1/Chassis": {
                "@odata.id": "/redfish/v1/Chassis/",
                "Members@odata.count": 6,
                "@odata.context": "/redfish/v1/$metadata#ChassisCollection.ChassisCollection",
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/Block-1"},
                    {"@odata.id": "/redfish/v1/Chassis/Block-2"},
                    {"@odata.id": "/redfish/v1/Chassis/Block-3"},
                    {"@odata.id": "/redfish/v1/Chassis/Rack-4"},
                    {"@odata.id": "/redfish/v1/Chassis/Rack-5"},
                    {"@odata.id": "/redfish/v1/Chassis/Sled-6"},
                ],
                "@odata.type": "#ChassisCollection.ChassisCollection",
                "Members@odata.navigationLink": "/redfish/v1/Chassis/Members",
                "@odata.etag": "W/\"aef74912345d8e2ae00d008591fc5d85\"",
                "Name": "ChassisCollection",
                "Description": "A Collection of Chassis resource instances."
            },
            "/redfish/v1/Chassis/Block-1": {
                "@odata.id": "/redfish/v1/Chassis/Block-1",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Block-2": {
                "@odata.id": "/redfish/v1/Chassis/Block-2",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Block-3": {
                "@odata.id": "/redfish/v1/Chassis/Block-3",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Rack-4": {
                "@odata.id": "/redfish/v1/Chassis/Rack-4",
                "ChassisType": "Rack",
            },
            "/redfish/v1/Chassis/Rack-5": {
                "@odata.id": "/redfish/v1/Chassis/Rack-5",
                "ChassisType": "Rack",
            },
            "/redfish/v1/Chassis/Sled-6": {
                "@odata.id": "/redfish/v1/Chassis/Sled-6",
                "ChassisType": "Sled",
            },
        })
        assert rf.num_chassis == 4

    def test_num_racks(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1": {
                "Chassis": {
                    "@odata.id": "/redfish/v1/Chassis"
                }
            },
            "/redfish/v1/Chassis": {
                "@odata.id": "/redfish/v1/Chassis/",
                "Members@odata.count": 6,
                "@odata.context": "/redfish/v1/$metadata#ChassisCollection.ChassisCollection",
                "Members": [
                    {"@odata.id": "/redfish/v1/Chassis/Block-1"},
                    {"@odata.id": "/redfish/v1/Chassis/Block-2"},
                    {"@odata.id": "/redfish/v1/Chassis/Block-3"},
                    {"@odata.id": "/redfish/v1/Chassis/Rack-4"},
                    {"@odata.id": "/redfish/v1/Chassis/Rack-5"},
                    {"@odata.id": "/redfish/v1/Chassis/Sled-6"},
                ],
                "@odata.type": "#ChassisCollection.ChassisCollection",
                "Members@odata.navigationLink": "/redfish/v1/Chassis/Members",
                "@odata.etag": "W/\"aef74912345d8e2ae00d008591fc5d85\"",
                "Name": "ChassisCollection",
                "Description": "A Collection of Chassis resource instances."
            },
            "/redfish/v1/Chassis/Block-1": {
                "@odata.id": "/redfish/v1/Chassis/Block-1",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Block-2": {
                "@odata.id": "/redfish/v1/Chassis/Block-2",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Block-3": {
                "@odata.id": "/redfish/v1/Chassis/Block-3",
                "ChassisType": "Enclosure",
            },
            "/redfish/v1/Chassis/Rack-4": {
                "@odata.id": "/redfish/v1/Chassis/Rack-4",
                "ChassisType": "Rack",
            },
            "/redfish/v1/Chassis/Rack-5": {
                "@odata.id": "/redfish/v1/Chassis/Rack-5",
                "ChassisType": "Rack",
            },
            "/redfish/v1/Chassis/Sled-6": {
                "@odata.id": "/redfish/v1/Chassis/Sled-6",
                "ChassisType": "Sled",
            },
        })
        assert rf.num_racks == 2

    def test_get_server(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        assert type(rf_server) == RedfishServer

    def test_get_chassis(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
            }
        })
        rf_chassis = rf.get_chassis("/redfish/v1/Chassis/Sled-1-2-1")
        assert type(rf_chassis) == RedfishChassis

    def test_get_rack(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Rack-1": {
                "@odata.id": "/redfish/v1/Chassis/Rack-1",
                "ChassisType": "Rack",
            }
        })
        rf_rack = rf.get_rack("/redfish/v1/Chassis/Rack-1")
        assert type(rf_rack) == RedfishRack

    def test_get_rack_bad(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
                "ChassisType": "Sled",
            }
        })
        with pytest.raises(InvalidValueException) as e:
            rf.get_rack("/redfish/v1/Chassis/Sled-1-2-1")
            assert e == "Chassis type Sled does not match that of a Rack"

    def test_server_stats_inventory(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                "Description": "A server",
                "Id": "System-1-2-1-1",
                "Manufacturer": "Dell Inc.",
                "MemorySummary": {
                    "TotalSystemMemoryGiB": 32,
                },
                "Name": "System",
                "PowerState": "On",
                "Processors": {
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors"
                },
                "SerialNumber": "945hjf0927mf",
            },
            "/redfish/v1/Systems/System-1-2-1-1/Processors": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors",
                "Members": [{
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1"
                }],
            },
            "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1",
                "InstructionSet": [{
                    "Member": "x86-64"
                }],
                "TotalCores": 20,
            }
        })
        physical_server = mock.Mock()
        physical_server.ems_ref = "/redfish/v1/Systems/System-1-2-1-1"
        requested_stats = ["cores_capacity", "memory_capacity",
                           #  "num_network_devices", "num_storage_devices"
                           ]
        requested_inventory = ["power_state"]
        assert (rf.server_stats(physical_server, requested_stats) == {
                "cores_capacity": 20, "memory_capacity": 32768})
        assert (rf.server_inventory(physical_server, requested_inventory) == {
                "power_state": "on"})

    def test_rack_stats_inventory(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Rack-1": {
                "@odata.id": "/redfish/v1/Chassis/Rack-1",
                "ChassisType": "Rack",
                "Description": "Redfish Rack",
                "Id": "Rack-1",
                "IndicatorLED": "Blinking",
                "Links": {
                    "ComputerSystems": [
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-2-1-1"
                        }
                    ]
                },
                "Manufacturer": "Dell",
                "Name": "G5_Rack",
                "SerialNumber": "1ABC",
            }
        })
        physical_rack = mock.Mock()
        physical_rack.ems_ref = "/redfish/v1/Chassis/Rack-1"
        requested_stats = []
        requested_inventory = ["rack_name"]
        assert rf.rack_stats(physical_rack, requested_stats) == {}
        assert (rf.rack_inventory(physical_rack, requested_inventory) == {
                "rack_name": "Rack-1"})

    def test_chassis_stats_inventory(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
                "ChassisType": "Sled",
                "Description": "G5 Sled-Level Enclosure",
                "Id": "Sled-1-2-1",
                "IndicatorLED": "Blinking",
                "Links": {
                    "ComputerSystems": [
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-2-1-1"
                        },
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-1-2-2"
                        }
                    ]
                },
                "Manufacturer": "Dell",
                "Name": "G5_Sled",
                "SerialNumber": "5555A",
            }
        })
        phsyical_chassis = mock.Mock()
        phsyical_chassis.ems_ref = "/redfish/v1/Chassis/Sled-1-2-1"
        requested_stats = ["num_physical_servers"]
        requested_inventory = ["chassis_name", "description", "identify_led_state"]
        assert (rf.chassis_stats(phsyical_chassis, requested_stats) == {
                "num_physical_servers": 2})
        assert (rf.chassis_inventory(phsyical_chassis, requested_inventory) == {
                "chassis_name": "Dell G5_Sled (5555A)",
                "description": "G5 Sled-Level Enclosure",
                "identify_led_state": "Blinking"})


class TestRedfishServer(RedfishTestCase):
    def test_server_simple_properties(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                "Description": "A server",
                "Id": "System-1-2-1-1",
                "Manufacturer": "Dell Inc.",
                "Name": "System",
                "PowerState": "On",
                "SerialNumber": "945hjf0927mf",
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        assert rf_server.name == "Dell Inc. System (945hjf0927mf)"
        assert rf_server.description == "A server"
        assert rf_server.state == "On"
        assert rf_server._identifying_attrs == {"odata_id": "/redfish/v1/Systems/System-1-2-1-1"}
        assert rf_server.uuid() == "System-1-2-1-1"

    def test_server_complex_properties(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                "Description": "A server",
                "Id": "System-1-2-1-1",
                "Manufacturer": "Dell Inc.",
                "MemorySummary": {
                    "TotalSystemMemoryGiB": 32,
                },
                "Name": "System",
                "PowerState": "On",
                "Processors": {
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors"
                },
                "SerialNumber": "945hjf0927mf",
            },
            "/redfish/v1/Systems/System-1-2-1-1/Processors": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors",
                "Members": [{
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1"
                }],
            },
            "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1/Processors/CPU.Socket.1",
                "InstructionSet": [{
                    "Member": "x86-64"
                }],
                "TotalCores": 20,
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        assert rf_server.server_cores == 20
        assert rf_server.server_memory == 32768
        assert rf_server.state == "On"
        assert rf_server._get_state() == ServerState.ON
        assert rf_server.is_on
        assert not rf_server.is_off
        assert not rf_server.is_powering_on
        assert not rf_server.is_powering_off
        assert rf_server.machine_type == "x86-64"
        assert rf_server.product_name == "System"

    def test_server_power_states(self):
        # string,
        #       is_on,  is_off, is_powering_on, is_powering_off
        test_data = {
            "On":
                [True,   False,  False,          False, ServerState.ON],  # noqa: E241
            "Off":
                [False,  True,   False,          False, ServerState.OFF],  # noqa: E241
            "PoweringOn":
                [False,  False,  True,           False, ServerState.POWERING_ON],  # noqa: E241
            "PoweringOff":
                [False,  False,  False,          True, ServerState.POWERING_OFF],  # noqa: E241
        }

        for str_state, states in test_data.items():
            rf = self.mock_redfish_system(self.mock_connector, data={
                "/redfish/v1/Systems/System-1-2-1-1": {
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                    "PowerState": str_state,
                }
            })
            rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
            assert rf_server.is_on == states[0]
            assert rf_server.is_off == states[1]
            assert rf_server.is_powering_on == states[2]
            assert rf_server.is_powering_off == states[3]
            assert rf_server._get_state() == states[4]

    def test_server_name_no_sn(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                "Id": "System-1-2-1-1",
                "Manufacturer": "Dell Inc.",
                "Name": "System",
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        assert rf_server.name == "Dell Inc. System"


class TestRedfishChassis(RedfishTestCase):
    def test_get_chassis_properties(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
                "ChassisType": "Sled",
                "Description": "G5 Sled-Level Enclosure",
                "Id": "Sled-1-2-1",
                "IndicatorLED": "Blinking",
                "Links": {
                    "ComputerSystems": [
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-2-1-1"
                        },
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-1-2-2"
                        }
                    ]
                },
                "Manufacturer": "Dell",
                "Name": "G5_Sled",
                "SerialNumber": "5555A",
            }
        })
        rf_chassis = rf.get_chassis("/redfish/v1/Chassis/Sled-1-2-1")
        assert rf_chassis.chassis_type == "Sled"
        assert rf_chassis.name == "Dell G5_Sled (5555A)"
        assert rf_chassis.description == "G5 Sled-Level Enclosure"
        assert rf_chassis.led_state == "Blinking"
        assert rf_chassis._identifying_attrs == {"odata_id": "/redfish/v1/Chassis/Sled-1-2-1"}
        assert rf_chassis.uuid() == "Sled-1-2-1"
        assert rf_chassis.num_servers == 2

    def test_get_rack_properties(self):
        rf = self.mock_redfish_system(self.mock_connector, data={
            "/redfish/v1/Chassis/Rack-1": {
                "@odata.id": "/redfish/v1/Chassis/Rack-1",
                "ChassisType": "Rack",
                "Description": "Redfish Rack",
                "Id": "Rack-1",
                "IndicatorLED": "Blinking",
                "Links": {
                    "ComputerSystems": [
                        {
                            "@odata.id": "/redfish/v1/Systems/System-1-2-1-1"
                        }
                    ]
                },
                "Manufacturer": "Dell",
                "Name": "G5_Rack",
                "SerialNumber": "1ABC",
            }
        })
        rf_rack = rf.get_rack("/redfish/v1/Chassis/Rack-1")
        assert rf_rack.chassis_type == "Rack"
        assert rf_rack.name == "Rack-1"
        assert rf_rack.description == "Redfish Rack"
        assert rf_rack.uuid() == "Rack-1"
        assert (rf_rack._identifying_attrs == {"odata_id":
            "/redfish/v1/Chassis/Rack-1"})
        assert rf_rack.led_state == "Blinking"
        assert rf_rack.num_servers == 1
