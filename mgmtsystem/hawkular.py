from base import MgmtSystemAPIBase
from collections import namedtuple
from rest_client import ContainerClient

import re
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
Resource = namedtuple('Resource', ['id', 'name', 'path'])
ResourceData = namedtuple('ResourceData', ['name', 'path', 'value'])
Server = namedtuple('Server', ['id', 'name', 'path', 'data'])
Deployment = namedtuple('Deployment', ['id', 'name', 'path'])
Datasource = namedtuple('Datasource', ['id', 'name', 'path'])
ServerStatus = namedtuple('ServerStatus', ['address', 'version', 'state', 'product', 'host'])
Event = namedtuple('event', ['id', 'eventType', 'ctime', 'dataSource', 'dataId',
                             'category', 'text'])

PATH_NAME_MAPPING = {
    '/t;': 'tenant',
    '/e;': 'environment',
    '/rt;': 'resource_type',
    '/mt;': 'metric_type',
    '/f;': 'feed',
    '/ot;': 'operation_type',
    '/mp;': 'metadata_pack',
    '/r;': 'resource',
    '/d;': 'data',
    '/rl;': 'relationship',
}


class Path(object):
    """Path class

    Path is class to split canonical path to friendly values.\
    If the path has more than one entry for a resource result will be in list
    Example:
        obj_p = Path('/t;28026b36-8fe4-4332-84c8-524e173a68bf\
        /f;88db6b41-09fd-4993-8507-4a98f25c3a6b\
        /r;Local~~/r;Local~%2Fdeployment%3Dhawkular-command-gateway-war.war')
        obj_p.path returns raw path
        obj_p.tenant returns tenant as `28026b36-8fe4-4332-84c8-524e173a68bf`
        obj_p.feed returns feed as `88db6b41-09fd-4993-8507-4a98f25c3a6b`
        obj_p.resource returns as \
        `[u'Local~~', u'Local~%2Fdeployment%3Dhawkular-command-gateway-war.war']`

    Args:
        path:   The canonical path. Example: /t;28026b36-8fe4-4332-84c8-524e173a68bf\
        /f;88db6b41-09fd-4993-8507-4a98f25c3a6b/r;Local~~

    """
    def __init__(self, path):
        self.paths = {'raw': path}
        if self.raw:
            raw_paths = re.split(r'(/\w+;)', self.raw)
            if len(raw_paths) % 2 == 1:
                del raw_paths[0]
            for p_index in range(0, len(raw_paths), 2):
                if PATH_NAME_MAPPING[raw_paths[p_index]] in self.paths:
                    if isinstance(self.paths[PATH_NAME_MAPPING[raw_paths[p_index]]], list):
                        self.paths[PATH_NAME_MAPPING[raw_paths[p_index]]] \
                            .append(raw_paths[p_index + 1])
                    else:
                        v_list = [
                            self.paths[PATH_NAME_MAPPING[raw_paths[p_index]]],
                            raw_paths[p_index + 1]
                        ]
                        self.paths.update({PATH_NAME_MAPPING[raw_paths[p_index]]: v_list})
                else:
                    self.paths.update(
                        {PATH_NAME_MAPPING[raw_paths[p_index]]: raw_paths[p_index + 1]})

    def __repr__(self):
        return self.paths['raw'] if 'raw' in self.paths else None

    def __getattr__(self, name):
        return self.paths[name] if name in self.paths else None


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
        'num_server': lambda self: len(self.list_server()),
        'num_deployment': lambda self: len(self.list_server_deployment()),
        'num_datasource': lambda self: len(self.list_server_datasource()),
    }

    def __init__(self,
            hostname, protocol="http", port=8080, **kwargs):
        super(Hawkular, self).__init__(kwargs)
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.auth = self.username, self.password
        self.inv_api = ContainerClient(hostname, self.auth, protocol, port, "hawkular/inventory")
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

    def list_server_deployment(self, **kwargs):
        """Returns list of server deployments. Possible filters: `feed_id`"""
        resources = self.list_resource(type_id='Deployment',
                                       feed_id=kwargs['feed_id'] if 'feed_id' in kwargs else None)
        deployments = []
        if resources:
            for resource in resources:
                deployments.append(Deployment(resource.id, resource.name, resource.path))
        return deployments

    def list_server(self, **kwargs):
        """Returns list of middleware servers. Possible filters: `feed_id`"""
        resources = self.list_resource(type_id='WildFly Server',
                                       feed_id=kwargs['feed_id'] if 'feed_id' in kwargs else None)
        servers = []
        if resources:
            for resource in resources:
                resource_data = self.resource_data(
                    feed_id=resource.path.feed, resource_id=resource.id)
                server_data = {'data_name': resource_data.name}
                server_data.update(resource_data.value)
                servers.append(Server(resource.id, resource.name, resource.path, server_data))
        return servers

    def list_resource(self, **kwargs):
        """Returns list of resources. Possible filters: `feed_id`, `type_id`"""
        feed_id = kwargs['feed_id'] if 'feed_id' in kwargs else None
        if not feed_id:
            resources = []
            feeds = self.list_feed()
            for feed in feeds:
                resources = \
                    resources + self._list_resource(type_id=kwargs['type_id'], feed_id=feed.id)
            return resources
        else:
            return self._list_resource(type_id=kwargs['type_id'], feed_id=feed_id)

    def _list_resource(self, **kwargs):
        """Returns list of resources by provided `type_id` and `feed_id`"""
        if not kwargs or 'feed_id' not in kwargs:
            raise KeyError('Variable "feed_id" is a mandatory field!')
        entities = []
        if kwargs['type_id']:
            entities_j = self.inv_api.get_json('feeds/{}/resourceTypes/{}/resources'
                                               .format(kwargs['feed_id'], kwargs['type_id']))
        else:
            entities_j = self.inv_api.get_json('feeds/{}/resources'.format(kwargs['feed_id']))
        if entities_j:
            for entity_j in entities_j:
                entities.append(Resource(entity_j['id'], entity_j['name'], Path(entity_j['path'])))
        return entities

    def resource_data(self, **kwargs):
        """Returns the data/configuration information about resource by provided\
         `feed_id` and `resource_id`."""
        if not kwargs or 'feed_id' not in kwargs or 'resource_id' not in kwargs:
            raise KeyError('Variable "feed_id" and "resource_id" are mandatory field!')
        entity_j = self.inv_api.get_json('feeds/{}/resources/{}/data'
                                     .format(kwargs['feed_id'], kwargs['resource_id']))
        if entity_j:
            return ResourceData(entity_j['name'], Path(entity_j['path']), entity_j['value'])
        return None

    def list_feed(self):
        """Returns list of feeds"""
        entities = []
        entities_j = self.inv_api.get_json('feeds')
        if entities_j:
            for entity_j in entities_j:
                entity = Feed(entity_j['id'],
                              entity_j['name'] if 'name' in entity_j else None,
                              Path(entity_j['path']))
                entities.append(entity)
        return entities

    def list_resource_type(self, **kwargs):
        """Returns list of resource types by provided `feed_id`"""
        if not kwargs or 'feed_id' not in kwargs:
            raise KeyError('Variable "feed_id" is a mandatory field!')
        entities = []
        entities_j = self.inv_api.get_json('feeds/{}/resourceTypes'.format(kwargs['feed_id']))
        if entities_j:
            for entity_j in entities_j:
                entity = ResourceType(entity_j['id'], entity_j['name'], entity_j['path'])
                entities.append(entity)
        return entities

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

    def list_server_datasource(self, **kwargs):
        """Returns list of datasources. Possible filters: `feed_id`"""
        resources = self.list_resource(type_id='Datasource',
                                       feed_id=kwargs['feed_id'] if 'feed_id' in kwargs else None)
        datasources = []
        if resources:
            for resource in resources:
                datasources.append(Datasource(resource.id, resource.name, resource.path))
        return datasources
