# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

import json

import requests
from requests.auth import HTTPBasicAuth

from wrapanapi.systems.base import System


class NuageSystem(System):
    """Client to Nuage API
    Args:
        hostname: The hostname of the system.
        username: The username to connect with.
        password: The password to connect with.
        api_port: The port to connect to.
        api_version: The api version, used as part of the url as-it-is.
        security_protocol: SSL or non-SSL
    """

    _stats_available = {
        'num_network_group': lambda self: len(self.list_network_groups()),
        'num_security_group': lambda self: len(self.list_security_groups()),
        'num_cloud_subnet': lambda self: len(self.list_cloud_subnets()),
    }

    def __init__(self, hostname, username, password, api_port, api_version, security_protocol,
                 **kwargs):
        super(NuageSystem, self).__init__(**kwargs)
        protocol = 'http' if 'non' in security_protocol.lower() else 'https'
        self.login_auth = (username, password)
        self.url = '{}://{}:{}/nuage/api/{}'.format(protocol, hostname, api_port, api_version)
        self._auth = None

    def info(self):
        return 'NuageSystem: url={}'.format(self.url)

    @property
    def auth(self):
        if not self._auth:
            login_url = self.url + "/me"
            response = requests.request('get', login_url, auth=HTTPBasicAuth(*self.login_auth),
                headers=self.common_headers, verify=False)
            response.raise_for_status()
            r = response.json()[0]
            self._auth = (r.get('userName'), r.get('APIKey'))
        return self._auth

    @property
    def common_headers(self):
        return {
            'Content-Type': 'application/json; charset=UTF-8',
            'X-Nuage-Organization': 'csp'
        }

    def list_network_groups(self):
        return self._request_list('/enterprises', 'get')

    def list_cloud_subnets(self):
        return self._request_list('/subnets', 'get', exclude_name='BackHaulSubnet')

    def list_security_groups(self):
        return self._request_list('/policygroups', 'get')

    def _request_list(self, *args, **kwargs):
        resp = self._request(*args, **kwargs)
        return [] if resp is None else resp

    def _request(self, url, method, data=None, exclude_name=None):
        headers = self.common_headers
        if exclude_name:
            headers.update({
                'X-Nuage-FilterType': 'predicate',
                'X-Nuage-Filter': "name ISNOT '{}'".format(exclude_name),
            })

        response = requests.request(
            method, self.url + url,
            auth=HTTPBasicAuth(*self.auth),
            headers=headers,
            verify=False,
            data=json.dumps(data) if data else None  # workaround Nuage bug that empty body "" is
        )                                            # returned when emtpy JSON "{}" should be
        response.raise_for_status()
        return response.json() if response.text else None
