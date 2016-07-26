from base import MgmtSystemAPIBase
from collections import namedtuple
from rest_client import ContainerClient
from urllib import quote as urlquote

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

Feed = namedtuple('Feed', ['id', 'path'])
ResourceType = namedtuple('ResourceType', ['id', 'name', 'path'])
Resource = namedtuple('Resource', ['id', 'name', 'path'])
ResourceData = namedtuple('ResourceData', ['name', 'path', 'value'])
Server = namedtuple('Server', ['id', 'name', 'path', 'data'])
Deployment = namedtuple('Deployment', ['id', 'name', 'path'])
Datasource = namedtuple('Datasource', ['id', 'name', 'path'])
OperationType = namedtuple('OperationType', ['id', 'name', 'path'])
ServerStatus = namedtuple('ServerStatus', ['address', 'version', 'state', 'product', 'host'])
Event = namedtuple('event', ['id', 'eventType', 'ctime', 'dataSource', 'dataId',
                             'category', 'text'])

CANONICAL_PATH_NAME_MAPPING = {
    '/d;': 'data_id',
    '/e;': 'environment_id',
    '/f;': 'feed_id',
    '/m;': 'metric_id',
    '/mp;': 'metadata_pack_id',
    '/mt;': 'metric_type_id',
    '/ot;': 'operation_type_id',
    '/r;': 'resource_id',
    '/rl;': 'relationship_id',
    '/rt;': 'resource_type_id',
    '/t;': 'tenant_id',
}


class CanonicalPath(object):
    """CanonicalPath class

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
        if path is None or len(path) == 0:
            raise KeyError("CanonicalPath should not be None or empty!")
        self._path_ids = []
        r_paths = re.split(r'(/\w+;)', path)
        if len(r_paths) % 2 == 1:
            del r_paths[0]
        for p_index in range(0, len(r_paths), 2):
            path_id = CANONICAL_PATH_NAME_MAPPING[r_paths[p_index]]
            path_value = r_paths[p_index + 1]
            if path_id in self._path_ids:
                if isinstance(getattr(self, path_id), list):
                    ex_list = getattr(self, path_id)
                    ex_list.append(path_value)
                    setattr(self, path_id, ex_list)
                else:
                    v_list = [
                        getattr(self, path_id),
                        path_value
                    ]
                    setattr(self, path_id, v_list)
            else:
                self._path_ids.append(path_id)
                setattr(self, path_id, path_value)

    def __iter__(self):
        """This enables you to iterate through like it was a dictionary, just without .iteritems"""
        for path_id in self._path_ids:
            yield (path_id, getattr(self, path_id))

    def __repr__(self):
        return "<CanonicalPath {}>".format(self.to_string)

    @property
    def to_string(self):
        c_path = ''
        if 'tenant_id' in self._path_ids:
            c_path = "/t;{}".format(self.tenant_id)
        if 'feed_id' in self._path_ids:
            c_path += "/f;{}".format(self.feed_id)
        if 'environment_id' in self._path_ids:
            c_path += "/e;{}".format(self.environment_id)
        if 'metric_id' in self._path_ids:
            c_path += "/m;{}".format(self.metric_id)
        if 'resource_id' in self._path_ids:
            if isinstance(self.resource_id, list):
                for _resource_id in self.resource_id:
                    c_path += "/r;{}".format(_resource_id)
            else:
                c_path += "/r;{}".format(self.resource_id)
        if 'metric_type_id' in self._path_ids:
            c_path += "/mt;{}".format(self.metric_type_id)
        if 'resource_type_id' in self._path_ids:
            c_path += "/rt;{}".format(self.resource_type_id)
        if 'metadata_pack_id' in self._path_ids:
            c_path += "/mp;{}".format(self.metadata_pack_id)
        if 'operation_type_id' in self._path_ids:
            c_path += "/ot;{}".format(self.operation_type_id)
        if 'relationship_id' in self._path_ids:
            c_path += "/rl;{}".format(self.relationship_id)
        return c_path


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
        self.tenant_id = kwargs.get('tenant_id', 'hawkular')
        self.auth = self.username, self.password
        self.inv_api = ContainerClient(hostname, self.auth, protocol, port, "hawkular/inventory")
        self.alerts_api = ContainerClient(hostname, self.auth, protocol, port, "hawkular/alerts")

    def _check_inv_version(self, version):
        return version in self._get_inv_json('status')['Implementation-Version']

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

    def list_server_deployment(self, feed_id=None):
        """Returns list of server deployments. Possible filters: `feed_id`"""
        resources = self.list_resource(feed_id=feed_id, resource_type_id='Deployment')
        deployments = []
        if resources:
            for resource in resources:
                deployments.append(Deployment(resource.id, resource.name, resource.path))
        return deployments

    def list_server(self, feed_id=None):
        """Returns list of middleware servers. Possible filters: `feed_id`"""
        resources = self.list_resource(feed_id=feed_id, resource_type_id='WildFly Server')
        servers = []
        if resources:
            for resource in resources:
                resource_data = self.get_config_data(
                    feed_id=resource.path.feed_id, resource_id=resource.id)
                server_data = resource_data.value
                servers.append(Server(resource.id, resource.name, resource.path, server_data))
        return servers

    def list_resource(self, resource_type_id, feed_id=None):
        """Returns list of resources. Possible filters: `feed_id`, `type_id`"""
        if not feed_id:
            resources = []
            for feed in self.list_feed():
                resources.extend(self._list_resource(feed_id=feed.path.feed_id,
                                                    resource_type_id=resource_type_id))
            return resources
        else:
            return self._list_resource(feed_id=feed_id, resource_type_id=resource_type_id)

    def list_child_resource(self, feed_id, resource_id, recursive=False):
        """Returns list of resources. Possible filters: `feed_id`, `type_id`"""
        if not feed_id or not resource_id:
            raise KeyError("'feed_id' and 'resource_id' are a mandatory field!")
        resources = []
        if recursive:
            entities_j = self._get_inv_json('traversal/f;{}/r;{}/recursive;over=isParentOf;type=r'
                                          .format(feed_id, resource_id))
        else:
            entities_j = self._get_inv_json('traversal/f;{}/r;{}/type=r'
                                            .format(feed_id, resource_id))
        if entities_j:
            for entity_j in entities_j:
                resources.append(Resource(entity_j['id'], entity_j['name'],
                                          CanonicalPath(entity_j['path'])))
        return resources

    def _list_resource(self, feed_id, resource_type_id=None):
        """Returns list of resources by provided `type_id` and `feed_id`"""
        if not feed_id:
            raise KeyError("'feed_id' is a mandatory field!")
        entities = []
        if resource_type_id:
            entities_j = self._get_inv_json('traversal/f;{}/rt;{}/rl;defines/type=r'
                                        .format(feed_id, resource_type_id))
        else:
            entities_j = self._get_inv_json('traversal/f;{}/type=r'.format(feed_id))
        if entities_j:
            for entity_j in entities_j:
                entities.append(Resource(entity_j['id'], entity_j['name'],
                                         CanonicalPath(entity_j['path'])))
        return entities

    def get_config_data(self, feed_id, resource_id):
        """Returns the data/configuration information about resource by provided\
         `feed_id` and `resource_id`."""
        if not feed_id or not resource_id:
            raise KeyError("'feed_id' and 'resource_id' are mandatory field!")
        entity_j = self._get_inv_json('entity/f;{}/r;{}/d;configuration'
                                      .format(feed_id, resource_id))
        if entity_j:
            return ResourceData(entity_j['name'], CanonicalPath(entity_j['path']),
                                entity_j['value'])
        return None

    def list_feed(self):
        """Returns list of feeds"""
        entities = []
        entities_j = self._get_inv_json('traversal/type=f')
        if entities_j:
            for entity_j in entities_j:
                entities.append(Feed(entity_j['id'], CanonicalPath(entity_j['path'])))
        return entities

    def list_resource_type(self, feed_id):
        """Returns list of resource types by provided `feed_id`"""
        if not feed_id:
            raise KeyError("'feed_id' is a mandatory field!")
        entities = []
        entities_j = self._get_inv_json('traversal/f;{}/type=rt'.format(feed_id))
        if entities_j:
            for entity_j in entities_j:
                entities.append(ResourceType(entity_j['id'], entity_j['name'], entity_j['path']))
        return entities

    def list_operation_definition(self, feed_id, resource_type_id):
        if feed_id is None or resource_type_id is None:
            raise KeyError("'feed_id' and 'resource_type_id' are mandatory fields!")
        res_j = self._get_inv_json('traversal/f;{}/rt;{}/type=ot'.format(feed_id, resource_type_id))
        operations = []
        if res_j:
            for res in res_j:
                operations.append(OperationType(res['id'], res['name'], CanonicalPath(res['path'])))
        return operations

    def list_server_datasource(self, feed_id=None):
        """Returns list of datasources. Possible filters: `feed_id`"""
        resources = self.list_resource(feed_id=feed_id, resource_type_id='Datasource')
        datasources = []
        if resources:
            for resource in resources:
                datasources.append(Datasource(resource.id, resource.name, resource.path))
        return datasources

    def edit_config_data(self, resource_data, **kwargs):
        """Edits the data.value information for resource by provided\
         `feed_id` and `resource_id`."""
        if not isinstance(resource_data, ResourceData) or not resource_data.value:
            raise KeyError(
                "'resource_data' should be ResourceData with 'value' attribute")
        if not kwargs or 'feed_id' not in kwargs or 'resource_id' not in kwargs:
            raise KeyError("'feed_id' and 'resource_id' are mandatory field!")
        r = self._put_inv_status('entity/f;{}/r;{}/d;configuration'
                .format(kwargs['feed_id'], kwargs['resource_id']), {"value": resource_data.value})
        return r

    def create_resource(self, resource, resource_data, resource_type, **kwargs):
        """Creates new resource and creates it's data by provided\
         `feed_id` and `resource_id`."""
        if not isinstance(resource, Resource):
            raise KeyError("'resource' should be an instance of Resource")
        if not isinstance(resource_data, ResourceData) or not resource_data.value:
            raise KeyError(
                "'resource_data' should be ResourceData with 'value' attribute")
        if not isinstance(resource_type, ResourceType):
            raise KeyError("'resource_type' should be an instance of ResourceType")
        if not kwargs or 'feed_id' not in kwargs:
            raise KeyError('Variable "feed_id" id mandatory field!')

        resource_id = urlquote(resource.id, safe='')
        r = self._post_inv_status('entity/f;{}/resource'.format(kwargs['feed_id']),
                                data={"name": resource.name, "id": resource.id,
                                "resourceTypePath": "rt;{}"
                                  .format(resource_type.path.resource_type_id)})
        if r:
            r = self._post_inv_status('entity/f;{}/r;{}/data'
                                    .format(kwargs['feed_id'], resource_id),
                                    data={'role': 'configuration', "value": resource_data.value})
        else:
            # if resource or it's data was not created correctly, delete resource
            self._delete_inv_status('entity/f;{}/r;{}'.format(kwargs['feed_id'], resource_id))
        return r

    def delete_resource(self, feed_id, resource_id):
        """Removed the resource by provided `feed_id` and `resource_id`."""
        if not feed_id or not resource_id:
            raise KeyError("'feed_id' and 'resource_id' are mandatory fields!")
        r = self._delete_inv_status('entity/f;{}/r;{}'.format(feed_id, resource_id))
        return r

    def list_event(self, start_time=0, end_time=sys.maxsize):
        """Returns the list of events filtered by provided start time and end time.
        Or lists all events if no argument provided.
        This information is wrapped into Event."""
        entities = []
        entities_j = self._get_alerts_json('events?startTime={}&endTime={}'
                                     .format(start_time, end_time))
        if entities_j:
            for entity_j in entities_j:
                entity = Event(entity_j['id'], entity_j['eventType'], entity_j['ctime'],
                               entity_j['dataSource'], entity_j['dataId'],
                               entity_j['category'], entity_j['text'])
                entities.append(entity)
        return entities

    def _get_inv_json(self, path):
        return self.inv_api.get_json(path, headers={"Hawkular-Tenant": self.tenant_id})

    def _post_inv_status(self, path, data):
        return self.inv_api.post_status(path, data,
                                        headers={"Hawkular-Tenant": self.tenant_id,
                                                "Content-Type": "application/json"})

    def _put_inv_status(self, path, data):
        return self.inv_api.put_status(path, data, headers={"Hawkular-Tenant": self.tenant_id,
                                                       "Content-Type": "application/json"})

    def _delete_inv_status(self, path):
        return self.inv_api.delete_status(path, headers={"Hawkular-Tenant": self.tenant_id})

    def _get_alerts_json(self, path):
        return self.alerts_api.get_json(path, headers={"Hawkular-Tenant": self.tenant_id})
