# coding: utf-8
from __future__ import absolute_import
from collections import namedtuple
from ironicclient import client as iclient
from keystoneauth1.identity import Password
from keystoneauth1.session import Session
from keystoneclient.v2_0 import client as oskclient
from novaclient import client as osclient
from novaclient.client import SessionClient
from requests.exceptions import Timeout

from wrapanapi.systems.base import System
from wrapanapi.exceptions import KeystoneVersionNotSupported


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
        return SessionClient.request(self, url, method, **kwargs)
    except Timeout:
        if retry_count >= 3:
            self._cfme_logger.error('nova request timed out after {} retries'.format(retry_count))
            raise
        else:
            # feed back into the replaced method that supports retry_count
            retry_count += 1
            self._cfme_logger.error('nova request timed out; retry {}'.format(retry_count))
            return self.request(url, method, retry_count=retry_count, **kwargs)


class OpenstackInfraSystem(System):
    """
    Openstack Infrastructure management system
    """

    _stats_available = {
        'num_template': lambda self: len(self.list_templates()),
        'num_host': lambda self: len(self.list_hosts()),
    }

    def __init__(self, **kwargs):
        self.keystone_version = kwargs.get('keystone_version', 2)
        if int(self.keystone_version) not in (2, 3):
            raise KeystoneVersionNotSupported(self.keystone_version)
        super(OpenstackInfraSystem, self).__init__(**kwargs)
        self.tenant = kwargs['tenant']
        self.username = kwargs['username']
        self.password = kwargs['password']
        self.auth_url = kwargs['auth_url']
        self.domain_id = kwargs['domain_id'] if self.keystone_version == 3 else None
        self._session = None
        self._api = None
        self._kapi = None
        self._capi = None
        self._iapi = None

    @property
    def _identifying_attrs(self):
        return {'auth_url': self.auth_url, 'tenant': self.tenant}

    @property
    def session(self):
        if not self._session:
            auth_kwargs = dict(auth_url=self.auth_url, username=self.username,
                               password=self.password, project_name=self.tenant)
            if self.keystone_version == 3:
                auth_kwargs.update(dict(user_domain_id=self.domain_id,
                                        project_domain_name=self.domain_id))
            pass_auth = Password(**auth_kwargs)
            self._session = Session(auth=pass_auth, verify=False)
        return self._session

    @property
    def api(self):
        if not self._api:
            self._api = osclient.Client('2',
                                        session=self.session,
                                        service_type="compute",
                                        insecure=True,
                                        timeout=30)
            # replace the client request method with our version that
            # can handle timeouts; uses explicit binding (versus
            # replacing the method directly on the SessionClient class)
            # so we can still call out to SessionClient's original request
            # method in the timeout handler method
            self._api.client._cfme_logger = self.logger
            self._api.client.request = _request_timeout_handler.__get__(
                self._api.client,
                SessionClient
            )
        return self._api

    @property
    def kapi(self):
        if not self._kapi:
            self._kapi = oskclient.Client(session=self.session, insecure=True)
        return self._kapi

    @property
    def iapi(self):
        if not self._iapi:
            self._iapi = iclient.get_client(1, session=self.session, insecure=True)
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
