# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
import json

import requests
from requests.auth import HTTPBasicAuth

import utils.json_utils as json_utils
from base import WrapanapiAPIBase


class NuageSystem(WrapanapiAPIBase):
    """Client to Nuage API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
    """

    NUAGE_HEADERS = {
        'X-Nuage-Organization': 'csp',
        "Content-Type": "application/json; charset=UTF-8"
    }

    NUAGE_API_PATH = "nuage/api/v5_0"

    _stats_available = {
        'num_enterprises': lambda self: len(self.list_enterprises()),
        'num_security_group': lambda self: len(self.list_security_groups()),
        'num_cloud_subnets': lambda self: len(self.list_cloud_subnets()),
        'num_domains': lambda self: len(self.list_domains()),
        'num_zones': lambda self: len(self.list_zones()),
        'num_vms': lambda self: len(self.list_vms()),
    }

    def __init__(self, hostname, username, password, protocol="https", port=443, **kwargs):
        super(NuageSystem, self).__init__(kwargs)
        self.login_auth = (username, password)
        self.url = '{}://{}:{}/'.format(protocol, hostname, port)
        self._auth = None

    @property
    def auth(self):
        if not self._auth:
            login_url = self.url + self.NUAGE_API_PATH + "/me"
            print "Calling " + login_url
            response = requests.request('get', login_url,
                                        auth=HTTPBasicAuth(self.login_auth[0], self.login_auth[1]),
                                        headers=self.NUAGE_HEADERS,
                                        verify=False
                                        )

            if response.ok:
                print "Status code = " + str(response.status_code)
                byteifyed_response_json = json_utils.json_loads_byteified(response.text)
                print byteifyed_response_json
                self._auth = (byteifyed_response_json[0].get('APIKey'),
                              byteifyed_response_json[0].get('enterpriseID'))
            else:
                raise StandardError(
                    "Login unsuccessful. Response status code: %s, response message: %s" %
                    (response.status_code, response.content)
                )
        return self._auth

    def list_enterprises(self):
        return self._request('/enterprises', 'get')

    def list_cloud_subnets(self):
        return self._request('/subnets', 'get')

    def list_policy_groups(self):
        return self._request('/policygroups', 'get')

    def list_domains(self):
        return self._request('/domains', 'get')

    def list_domains_for_enterprise(self, enterprise_id):
        return self._request("/enterprises/%s/domains" % enterprise_id, 'get')

    def list_zones(self):
        return self._request('/zones', 'get')

    def list_vms(self):
        return self._request('/vms', 'get')

    def _request(self, url, method, data=None):
        response = requests.request(method, self.url + self.NUAGE_API_PATH + url,
                                    auth=HTTPBasicAuth(self.login_auth[0], self.auth[0]),
                                    headers=self.NUAGE_HEADERS,
                                    verify=False,
                                    data=json.dumps(data) if data else None
                                    )
        return response
