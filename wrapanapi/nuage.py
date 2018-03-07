# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import
import json

import requests
from requests.auth import HTTPBasicAuth
from .base import WrapanapiAPIBase


class NuageSystem(WrapanapiAPIBase):
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
        super(NuageSystem, self).__init__(kwargs)
        protocol = 'http' if 'non' in security_protocol.lower() else 'https'
        self.login_auth = (username, password)
        self.url = '{}://{}:{}/nuage/api/{}'.format(protocol, hostname, api_port, api_version)
        self._auth = None

        self.enterprise_name = 'Integration Tests - Enterprise'
        self.domain_template_name = 'Integration Tests - Domain Template'
        self.domain_name = 'Integration Tests - Domain'
        self.zone_name = 'Integration Tests - Zone'
        self.subnet_name = 'Integration Tests - Subnet'
        self._enterprise = None
        self._domain_template = None
        self._domain = None
        self._zone = None
        self._subnet = None
        self.entity_description = 'This object was created automatically by Integration Tests.'

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

    #
    # Integration-test specific entities on Nuage server
    #

    @property
    def enterprise(self):
        if not self._enterprise:
            match = self.list_enterprises(name=self.enterprise_name)
            if match:
                self._enterprise = match[0]['ID']
        return self._enterprise

    @property
    def domain_template(self):
        if not self._domain_template:
            match = self.list_domain_templates(name=self.domain_template_name)
            if match:
                self._domain_template = match[0]['ID']
        return self._domain_template

    @property
    def domain(self):
        if not self._domain:
            match = self.list_domains_for_enterprise(name=self.domain_name)
            if match:
                self._domain = match[0]['ID']
        return self._domain

    @property
    def zone(self):
        if not self._zone:
            match = self.list_zones_for_domain(name=self.zone_name)
            if match:
                self._zone = match[0]['ID']
        return self._zone

    @property
    def subnet(self):
        if not self._subnet:
            match = self.list_cloud_subnets(name=self.subnet_name)
            if match:
                self._subnet = match[0]['ID']
        return self._subnet

    #
    # Multi API calls wrappers
    #

    def destroy_enterprise(self, name=None):
        """Delete enterprise by name if exists, even if it's non-empty"""
        if not name:
            name = self.enterprise_name
        enterprise = [etp['ID'] for etp in self.list_enterprises() if etp['name'] == name]
        if not enterprise:  # enterprise doesn't exist so we are done
            return
        else:
            enterprise = enterprise[0]
        for domain in self.list_domains_for_enterprise(enterprise):
            self.delete_domain(domain['ID'])
        self.delete_enterprise(enterprise)

    #
    # API calls
    #

    def list_network_groups(self):
        return self._request_list('/enterprises', 'get')

    def list_cloud_subnets(self, name=None):
        return self._request_list(
            '/subnets',
            'get',
            exclude_name='BackHaulSubnet',
            search_name=name
        ) or []

    def list_cloud_subnets_for_domain(self, domain=None):
        if not domain:
            domain = self.domain
        return self._request_list('/domains/{}/subnets'.format(domain), 'get') or []

    def list_cloud_subnets_for_zone(self, zone):
        return self._request_list('/zones/{}/subnets'.format(zone), 'get') or []

    def list_security_groups(self):
        return self._request_list('/policygroups', 'get')

    def list_enterprises(self, name=None):
        return self._request_list('/enterprises', 'get', search_name=name) or []

    def list_domain_templates(self, enterprise=None, name=None):
        if not enterprise:
            enterprise = self.enterprise
        return self._request_list('/enterprises/{}/domaintemplates'.format(enterprise), 'get',
                             search_name=name) or []

    def list_domains(self, name=None):
        return self._request_list('/domains', 'get', search_name=name) or []

    def list_domains_for_enterprise(self, enterprise=None, name=None):
        if not enterprise:
            enterprise = self.enterprise
        return self._request_list(
            '/enterprises/{}/domains'.format(enterprise),
            'get',
            search_name=name
        ) or []

    def list_zones_for_domain(self, domain=None, name=None):
        if not domain:
            domain = self.domain
        return self._request_list('/domains/{}/zones'.format(domain), 'get', search_name=name) or []

    def create_enterprise(self):
        return self._request('/enterprises', 'post', data={
            'name': self.enterprise_name,
            'description': self.entity_description
        })

    def create_domain_template(self, enterprise=None):
        if not enterprise:
            enterprise = self.enterprise
        return self._request('/enterprises/{}/domaintemplates'.format(enterprise), 'post', data={
            'name': self.domain_template_name,
            'description': self.entity_description
        })

    def create_domain(self, enterprise=None, domain_template=None):
        if not enterprise:
            enterprise = self.enterprise
        if not domain_template:
            domain_template = self.domain_template
        return self._request('/enterprises/{}/domains'.format(enterprise), 'post', data={
            'name': self.domain_name,
            'description': self.entity_description,
            'templateID': domain_template
        })

    def create_zone(self, domain=None):
        if not domain:
            domain = self.domain
        return self._request('/domains/{}/zones'.format(domain), 'post', data={
            'name': self.zone_name,
            'description': self.entity_description
        })

    def create_subnet(self, zone=None):
        if not zone:
            zone = self.zone
        return self._request('/zones/{}/subnets'.format(zone), 'post', data={
            'name': self.subnet_name,
            'description': self.entity_description,
            'address': '193.192.191.0',
            'gateway': '193.192.191.1',
            'netmask': '255.255.255.0',
            'multicast': 'INHERITED',
            'IPType': 'IPV4'
        })

    def delete_subnet(self, subnet=None):
        if not subnet:
            subnet = self.subnet
        return self._request('/subnets/{}'.format(subnet), 'delete')

    def delete_zone(self, zone=None):
        if not zone:
            zone = self.zone
        return self._request('/zones/{}'.format(zone), 'delete')

    def delete_domain(self, domain=None):
        if not domain:
            domain = self.domain
        return self._request('/domains/{}'.format(domain), 'delete')

    def delete_domain_template(self, domain_template=None):
        if not domain_template:
            domain_template = self.domain_template
        return self._request('/domaintemplates/{}'.format(domain_template), 'delete')

    def delete_enterprise(self, enterprise=None):
        if not enterprise:
            enterprise = self.enterprise
        return self._request('/enterprises/{}?responseChoice=1'.format(enterprise), 'delete')

    def _request_list(self, *args, **kwargs):
        resp = self._request(*args, **kwargs)
        return [] if resp is None else resp

    def _request(self, url, method, data=None, exclude_name=None, search_name=None):
        headers = self.common_headers
        if exclude_name:
            headers.update({
                'X-Nuage-FilterType': 'predicate',
                'X-Nuage-Filter': "name ISNOT '{}'".format(exclude_name),
            })
        if search_name:
            headers.update({
                'X-Nuage-FilterType': 'predicate',
                'X-Nuage-Filter': "name IS '{}'".format(search_name),
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
