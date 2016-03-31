from collections import namedtuple
from rest_client import ContainerClient

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


class Hawkular(object):
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

    def __init__(self,
            hostname, protocol="http", port=8080, entry="hawkular/inventory", **kwargs):
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.auth = self.username, self.password
        self.api = ContainerClient(hostname, self.auth, protocol, port, entry)

    def list_server_deployment(self, feed_id, type_id='Deployment'):
        """Returns list of deployments on servers by provided feed ID and resource type ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes/{}/resources'
                                       .format(feed_id, type_id))
        for entity_j in entities_j:
            entity = Server(entity_j['id'], entity_j['name'], entity_j['path'])
            entities.append(entity)
        return entities

    def list_feed(self):
        """Returns list of feeds"""
        entities = []
        entities_j = self.api.get_json('feeds')
        for entity_j in entities_j:
            entity = Feed(entity_j['id'],
                          entity_j['name'] if entity_j.__contains__('name') else None,
                          entity_j['path'])
            entities.append(entity)
        return entities

    def list_resource_type(self, feed_id):
        """Returns list of resource types by provided feed ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes'.format(feed_id))
        for entity_j in entities_j:
            entity = ResourceType(entity_j['id'], entity_j['name'], entity_j['path'])
            entities.append(entity)
        return entities

    def list_server(self, feed_id, type_id='WildFly Server'):
        """Returns list of middleware servers by provided feed ID and resource type ID"""
        entities = []
        entities_j = self.api.get_json('feeds/{}/resourceTypes/{}/resources'
                                       .format(feed_id, type_id))
        for entity_j in entities_j:
            entity = Server(entity_j['id'], entity_j['name'], entity_j['path'])
            entities.append(entity)
        return entities
