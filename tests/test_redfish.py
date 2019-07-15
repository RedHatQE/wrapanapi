# coding: utf-8
# Unit tests for the Redfish API
from unittest import mock, TestCase

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


@mock.patch("redfish_client.Connector")
class TestRedfishSystem(TestCase):
    def mock_redfish_system(self, mock_connector, data):
        def mock_get(url):
            res = mock.Mock()
            res.status = 200
            res.json = data.get(url, {})
            return res

        mock_connector.get.side_effect = mock_get
        api_client = Root(mock_connector)

        rf = RedfishSystem("dummy", "user", "pass", "Non-SSL", "8000",
            api_client=api_client)
        return rf

    def test_find_resource(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/BlankResource": {
                "@odata.id": "id"
            }
        })
        resource = rf.find("/redfish/v1/BlankResource")
        self.assertEqual(resource.raw, {"@odata.id": "id"})

    def test_get_server(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        self.assertEqual(type(rf_server), RedfishServer)

    def test_server_simple_properties(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
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
        self.assertEqual(rf_server.name, "Dell Inc. System (945hjf0927mf)")
        self.assertEqual(rf_server.description, "A server")
        self.assertEqual(rf_server.state, "On")
        self.assertEqual(rf_server._identifying_attrs,
            {"odata_id": "/redfish/v1/Systems/System-1-2-1-1"})
        self.assertEqual(rf_server.uuid(), "System-1-2-1-1")

    def test_server_complex_properties(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
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
                "TotalCores": 20,
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        self.assertEqual(rf_server.server_cores, 20)
        self.assertEqual(rf_server.server_memory, 32768)
        self.assertEqual(rf_server.state, "On")
        self.assertEqual(rf_server._get_state(), ServerState.ON)
        self.assertTrue(rf_server.is_on)
        self.assertFalse(rf_server.is_off)
        self.assertFalse(rf_server.is_powering_on)
        self.assertFalse(rf_server.is_powering_off)

    def test_server_power_states(self, mock_connector):
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
            rf = self.mock_redfish_system(mock_connector, data={
                "/redfish/v1/Systems/System-1-2-1-1": {
                    "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                    "PowerState": str_state,
                }
            })
            rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
            self.assertEqual(rf_server.is_on, states[0])
            self.assertEqual(rf_server.is_off, states[1])
            self.assertEqual(rf_server.is_powering_on, states[2])
            self.assertEqual(rf_server.is_powering_off, states[3])
            self.assertEqual(rf_server._get_state(), states[4])

    def test_server_name_no_sn(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/Systems/System-1-2-1-1": {
                "@odata.id": "/redfish/v1/Systems/System-1-2-1-1",
                "Id": "System-1-2-1-1",
                "Manufacturer": "Dell Inc.",
                "Name": "System",
            }
        })
        rf_server = rf.get_server("/redfish/v1/Systems/System-1-2-1-1")
        self.assertEqual(rf_server.name, "Dell Inc. System")

    def test_get_chassis(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
            }
        })
        rf_chassis = rf.get_chassis("/redfish/v1/Chassis/Sled-1-2-1")
        self.assertEqual(type(rf_chassis), RedfishChassis)

    def test_get_chassis_properties(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
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
        self.assertEqual(rf_chassis.chassis_type, "Sled")
        self.assertEqual(rf_chassis.name, "Dell G5_Sled (5555A)")
        self.assertEqual(rf_chassis.description, "G5 Sled-Level Enclosure")
        self.assertEqual(rf_chassis.led_state, "Blinking")
        self.assertEqual(rf_chassis._identifying_attrs,
            {"odata_id": "/redfish/v1/Chassis/Sled-1-2-1"})
        self.assertEqual(rf_chassis.uuid(), "Sled-1-2-1")
        self.assertEqual(rf_chassis.num_servers, 2)

    def test_get_rack(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/Chassis/Rack-1": {
                "@odata.id": "/redfish/v1/Chassis/Rack-1",
                "ChassisType": "Rack",
            }
        })
        rf_rack = rf.get_rack("/redfish/v1/Chassis/Rack-1")
        self.assertEqual(type(rf_rack), RedfishRack)

    def test_get_rack_bad(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
            "/redfish/v1/Chassis/Sled-1-2-1": {
                "@odata.id": "/redfish/v1/Chassis/Sled-1-2-1",
                "ChassisType": "Sled",
            }
        })
        with self.assertRaises(InvalidValueException,
                msg="Chassis type Sled does not match that of a Rack"):
            rf.get_rack("/redfish/v1/Chassis/Sled-1-2-1")

    def test_get_rack_properties(self, mock_connector):
        rf = self.mock_redfish_system(mock_connector, data={
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
        self.assertEqual(rf_rack.chassis_type, "Rack")
        self.assertEqual(rf_rack.name, "Rack-1")
        self.assertEqual(rf_rack.description, "Redfish Rack")
        self.assertEqual(rf_rack.uuid(), "Rack-1")
        self.assertEqual(rf_rack._identifying_attrs, {"odata_id":
            "/redfish/v1/Chassis/Rack-1"})
        self.assertEqual(rf_rack.led_state, "Blinking")
        self.assertEqual(rf_rack.num_servers, 1)
