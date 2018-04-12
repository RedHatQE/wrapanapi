# coding: utf-8
from __future__ import absolute_import
from collections import namedtuple
from ironicclient import client as iclient
from keystoneclient.v2_0 import client as oskclient
from novaclient import client as osclient
from novaclient.client import HTTPClient
from requests.exceptions import Timeout

from wrapanapi.systems import System


Node = namedtuple('Node', ['uuid', 'name', 'power_state', 'provision_state'])


# TODO The following monkeypatch nonsense is criminal, and would be
# greatly simplified if openstack made it easier to specify a custom
# client class. This is a trivial PR that they're likely to accept.

# Note: This same mechanism may be required for keystone and cinder
# clients, but hopefully won't be.

# monkeypatch method to add retry support to openstack
def _request_timeout_handler(self, url, method, retry_count=0, **kwargs):
    try:
        # Use the original request method to do the actual work
        return HTTPClient.request(self, url, method, **kwargs)
    except Timeout:
        if retry_count >= 3:
            self._cfme_logger.error('nova request timed out after {} retries'.format(retry_count))
            raise
        else:
            # feed back into the replaced method that supports retry_count
            retry_count += 1
            self._cfme_logger.error('nova request timed out; retry {}'.format(retry_count))
            return self.request(url, method, retry_count=retry_count, **kwargs)


class OpenstackInfraSystem(System):  # TODO: possibly add EntityMixin's
    """Openstack Infrastructure management system

    # TODO
    """

    _stats_available = {
        'num_template': lambda self: len(self.list_templates()),
        'num_host': lambda self: len(self.list_hosts()),
    }

    states = {
        'running': ('ACTIVE',),
        'stopped': ('SHUTOFF',),
        'suspended': ('SUSPENDED',),
    }

    can_suspend = True

    def __init__(self, **kwargs):
        super(OpenstackInfraSystem, self).__init__(kwargs)
        self.tenant = kwargs['tenant']
        self.username = kwargs['username']
        self.password = kwargs['password']
        self.auth_url = kwargs['auth_url']
        self._api = None
        self._kapi = None
        self._capi = None
        self._iapi = None

    @property
    def api(self):
        if not self._api:
            self._api = osclient.Client('2',
                                        self.username,
                                        self.password,
                                        self.tenant,
                                        self.auth_url,
                                        service_type="compute",
                                        insecure=True,
                                        timeout=30)
            # replace the client request method with our version that
            # can handle timeouts; uses explicit binding (versus
            # replacing the method directly on the HTTPClient class)
            # so we can still call out to HTTPClient's original request
            # method in the timeout handler method
            self._api.client._cfme_logger = self.logger
            self._api.client.request = _request_timeout_handler.__get__(self._api.client,
                HTTPClient)
        return self._api

    @property
    def kapi(self):
        if not self._kapi:
            self._kapi = oskclient.Client(username=self.username,
                                          password=self.password,
                                          tenant_name=self.tenant,
                                          auth_url=self.auth_url,
                                          insecure=True)
        return self._kapi

    @property
    def iapi(self):
        if not self._iapi:
            self._iapi = iclient.get_client(
                1,
                os_auth_url=self.auth_url,
                os_username=self.username,
                os_password=self.password,
                os_project_name=self.tenant,
                insecure=True)
        return self._iapi

    @property
    def nodes(self):
        return self.api.servers.list()

    @property
    def images(self):
        return self.api.images.list()

    @property
    def networks(self):
        return self.api.networks.list()

    def list_templates(self):
        return [image.name for image in self.images]

    def list_networks(self):
        return [network.name for network in self.networks]

    def list_hosts(self):
        return [node.name for node in self.nodes]

    def list_nodes(self):
        """Query Ironic for the node info. Where possible, obtain the name from nova."""
        nodes = self.nodes
        result = []
        for i_node in self.iapi.node.list():
            if i_node.name:
                name = i_node.name
            else:
                # Sometimes Ironic does not show the names, pull them from Nova if possible.
                selected_nova_node = None
                for nova_node in nodes:
                    if getattr(
                            nova_node, 'OS-EXT-SRV-ATTR:hypervisor_hostname', None) == i_node.uuid:
                        selected_nova_node = nova_node
                        break
                if selected_nova_node:
                    name = selected_nova_node.name
                else:
                    name = None
            result.append(Node(i_node.uuid, name, i_node.power_state, i_node.provision_state))
        return result

    def info(self):
        raise NotImplementedError('info not implemented.')

    def disconnect(self):
        pass