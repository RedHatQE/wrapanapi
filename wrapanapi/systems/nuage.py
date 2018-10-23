# coding: utf-8
"""Backend management system classes
Used to communicate with providers without using CFME facilities
"""
from __future__ import absolute_import

from wrapanapi.systems.base import System
from wrapanapi.utils.random import random_name


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
        # We're returning 3rd element of .count() tuple which is formed as
        # entities.count() == (fetcher, served object, count of fetched objects)
        'num_security_group': lambda self: self.api.policy_groups.count()[2],
        # Filter out 'BackHaulSubnet' and combine it with l2_domains the same way CloudForms does
        'num_cloud_subnet': lambda self: self.api.subnets.count(
            filter="name != 'BackHaulSubnet'")[2] + self.api.l2_domains.count()[2],
        'num_cloud_tenant': lambda self: self.api.enterprises.count()[2],
        'num_network_router': lambda self: self.api.domains.count()[2],
        'num_cloud_network': lambda self: len(self.list_floating_network_resources()),
        'num_floating_ip': lambda self: self.api.floating_ips.count()[2],
        'num_network_port': lambda self: len(self.list_vports())
    }

    def __init__(self, hostname, username, password, api_port, api_version, security_protocol,
                 **kwargs):
        super(NuageSystem, self).__init__(**kwargs)
        protocol = 'http' if 'non' in security_protocol.lower() else 'https'
        self.username = username
        self.password = password
        self.url = '{}://{}:{}'.format(protocol, hostname, api_port)
        self.enterprise = 'csp'
        self.api_version = api_version
        self._api = None

    @property
    def vspk(self):
        if self.api_version == 'v4_0':
            from vspk import v4_0 as vspk
        else:
            from vspk import v5_0 as vspk
        return vspk

    @property
    def api(self):
        if self._api is None:
            session = self.vspk.NUVSDSession(
                username=self.username,
                password=self.password,
                enterprise=self.enterprise,
                api_url=self.url
            )
            session.start()
            self._api = session.user
        return self._api

    def disconnect(self):
        self._api = None

    @property
    def _identifying_attrs(self):
        return {'url': self.url}

    def info(self):
        return 'NuageSystem: url={}'.format(self.url)

    def list_floating_network_resources(self):
        return self.api.shared_network_resources.get(filter='type is "FLOATING"')

    def list_vports(self):
        # The vspk module is a bit specific because you first need to explicitly fetch child
        # objects before accessing them. Docs say you should do it like this:
        #    domain.vports.get()
        #    for vport in domain.vports: ...
        # We do a nasty trick here to inline it: `domain.vports.get() and domain.vports`
        vports = [d.vports.get() and d.vports for d in self.api.domains.get()]
        vports.extend([d.vports.get() and d.vports for d in self.api.l2_domains.get()])
        return [vport for sublist in vports for vport in sublist]

    def create_enterprise(self, name=None):
        enterprise, _ = self.api.create_child(self.vspk.NUEnterprise(name=name or random_name()))
        return enterprise

    def delete_enterprise(self, enterprise):
        enterprise.domains.get()
        enterprise.l2_domains.get()

        objects = enterprise.domains
        objects.extend(enterprise.l2_domains)
        objects.append(enterprise)

        for obj in objects:
            obj.delete()
