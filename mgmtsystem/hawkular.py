from base import MgmtSystemAPIBase
from collections import namedtuple
from rest_client import ContainerClient

import sys

"""
Related yaml structures:

[cfme_data]
management_systems:
    hawkular:
        name: My hawkular
        type: hawkular
        hostname: 10.12.13.14
        port: 8080
        credentials: hawkular
        authenticate: true
        rest_protocol: http

[credentials]
hawkular:
    username: admin
    password: secret
"""

Feed = namedtuple('Feed', ['id', 'name', 'path'])
ResourceType = namedtuple('ResourceType', ['id', 'name', 'path'])
Server = namedtuple('Server', ['id', 'name', 'path'])
Deployment = namedtuple('Deployment', ['id', 'name', 'path'])
Datasource = namedtuple('Datasource', ['id', 'name', 'path'])
ServerStatus = namedtuple('ServerStatus', ['address', 'version', 'state', 'product', 'host'])
Event = namedtuple('event', ['id', 'eventType', 'ctime', 'dataSource', 'dataId',
                             'category', 'text'])


class Hawkular(MgmtSystemAPIBase):
    """Hawkular management system

    Hawkular REST API method calls.
    Will be used by cfme_tests project to verify Hawkular content shown in CFME UI

    Args:
        hostname: The Hawkular hostname.
        protocol: Hawkular REST API protocol. Default value: 'http'
        port: Hawkular REST API port on provided host. Default value: '8080'.
        entry: Hawkular REST API entry point URI. Default value: 'hawkular/inventory'
        username: The username to connect with.
        password: The password to connect with.

    """

    _stats_available = {
        'num_server': lambda self: sum(len(self.list_server(f.id)) for f in self.list_feed()),
        'num_deployment': lambda self: sum(len(self.list_server_deployment(f.id))
                                           for f in self.list_feed()),
        'num_datasource': lambda self: sum(len(self.list_server_datasource(f.id))
                                           for f in self.list_feed()),
    }

    def __init__(self,
            hostname, protocol="http", port=8080, **kwargs):
        super(Hawkular, self).__init__(kwargs)
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.auth = self.username, self.password
        self.api = ContainerClient(hostname, self.auth, protocol, port, "hawkular/inventory")
        self.alerts_api = ContainerClient(hostname, self.auth, protocol, port, "hawkular/alerts")

    def info(self):
        raise NotImplementedError('info not implemented.')

    def clone_vm(self, source_name, vm_name):
        raise NotImplementedError('clone_vm not implemented.')

    def create_vm(self, vm_name):
        raise NotImplementedError('create_vm not implemented.')

    def current_ip_address(self, vm_name):
        raise NotImplementedError('current_ip_address not implemented.')

    def delete_vm(self, vm_name):
        raise NotImplementedError('delete_vm not implemented.')

    def deploy_template(self, template, *args, **kwargs):
        raise NotImplementedError('deploy_template not implemented.')

    def disconnect(self):
        pass

    def does_vm_exist(self, name):
        raise NotImplementedError('does_vm_exist not implemented.')

    def get_ip_address(self, vm_name):
        raise NotImplementedError('get_ip_address not implemented.')

    def is_vm_running(self, vm_name):
        raise NotImplementedError('is_vm_running not implemented.')

    def is_vm_stopped(self, vm_name):
        raise NotImplementedError('is_vm_stopped not implemented.')

    def is_vm_suspended(self, vm_name):
        raise NotImplementedError('is_vm_suspended not implemented.')

    def list_flavor(self):
        raise NotImplementedError('list_flavor not implemented.')

    def list_template(self):
        raise NotImplementedError('list_template not implemented.')

    def list_vm(self, **kwargs):
        raise NotImplementedError('list_vm not implemented.')

    def remove_host_from_cluster(self, hostname):
        raise NotImplementedError('remove_host_from_cluster not implemented.')

    def restart_vm(self, vm_name):
        raise NotImplementedError('restart_vm not implemented.')

    def start_vm(self, vm_name):
        raise NotImplementedError('start_vm not implemented.')

    def stop_vm(self, vm_name):
        raise NotImplementedError('stop_vm not implemented.')

    def suspend_vm(self, vm_name):
        raise NotImplementedError('restart_vm not implemented.')

    def vm_status(self, vm_name):
        raise NotImplementedError('vm_status not implemented.')

    def wait_vm_running(self, vm_name, num_sec):
        raise NotImplementedError('wait_vm_running not implemented.')

    def wait_vm_stopped(self, vm_name, num_sec):
        raise NotImplementedError('wait_vm_stopped not implemented.')

    def wait_vm_suspended(self, vm_name, num_sec):
        raise NotImplementedError('wait_vm_suspended not implemented.')

    def list_server_deployment(self, feed_id, type_id='Deployment'):
        """Returns list of deployments on servers by provided feed ID and resource type ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes/{}/resources'
                                       .format(feed_id, type_id))
        if entities_j:
            for entity_j in entities_j:
                entity = Deployment(entity_j['id'], entity_j['name'], entity_j['path'])
                entities.append(entity)
        return entities

    def list_feed(self):
        """Returns list of feeds"""
        entities = []
        entities_j = self.api.get_json('feeds')
        if entities_j:
            for entity_j in entities_j:
                entity = Feed(entity_j['id'],
                          entity_j['name'] if 'name' in entity_j else None,
                          entity_j['path'])
                entities.append(entity)
        return entities

    def list_resource_type(self, feed_id):
        """Returns list of resource types by provided feed ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes'.format(feed_id))
        if entities_j:
            for entity_j in entities_j:
                entity = ResourceType(entity_j['id'], entity_j['name'], entity_j['path'])
                entities.append(entity)
        return entities

    def list_server(self, feed_id, type_id='WildFly Server'):
        """Returns list of middleware servers by provided feed ID and resource type ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes/{}/resources'
                                       .format(feed_id, type_id))
        if entities_j:
            for entity_j in entities_j:
                entity = Server(entity_j['id'], entity_j['name'], entity_j['path'])
                entities.append(entity)
        return entities

    def get_server_status(self, feed_id, resource_id):
        """Returns the data info about resource by provided feed ID and resource ID.
        This information is wrapped into ServerStatus."""
        entity_j = self.api.get_json('feeds/{}/resources/{}/data'
                                     .format(feed_id, resource_id))
        if entity_j:
            value_j = entity_j['value']
            if value_j:
                entity = ServerStatus(value_j['Bound Address'], value_j['Version'],
                                      value_j['Server State'], value_j['Product Name'],
                                      value_j['Hostname'])
                return entity
        return None

    def list_event(self, start_time=0, end_time=sys.maxsize):
        """Returns the list of events filtered by provided start time and end time.
        Or lists all events if no argument provided.
        This information is wrapped into Event."""
        entities = []
        entities_j = self.alerts_api.get_json('events?startTime={}&endTime={}'
                                     .format(start_time, end_time))
        if entities_j:
            for entity_j in entities_j:
                entity = Event(entity_j['id'], entity_j['eventType'], entity_j['ctime'],
                               entity_j['dataSource'], entity_j['dataId'],
                               entity_j['category'], entity_j['text'])
                entities.append(entity)
        return entities

    def list_server_datasource(self, feed_id, type_id='Datasource'):
        """Returns list of datasources on servers by provided feed ID and resource type ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes/{}/resources'
                                       .format(feed_id, type_id))
        if entities_j:
            for entity_j in entities_j:
                entity = Datasource(entity_j['id'], entity_j['name'], entity_j['path'])
                entities.append(entity)
        return entities
