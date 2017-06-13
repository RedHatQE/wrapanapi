from collections import namedtuple
from kubernetes import Kubernetes, ImageRegistry, Project, Service
from rest_client import ContainerClient

"""
Related yaml structures:

[cfme_data]
management_systems:
    openshift:
        name: My openshift
        type: openshift
        hostname: 10.12.13.14
        port: 8443
        credentials: openshift
        authenticate: true
        rest_protocol: https

[credentials]
openshift:
    username: admin
    password: secret
    token: mytoken
"""

Route = namedtuple('Route', ['name', 'project_name'])
Template = namedtuple('Template', ['name', 'project_name'])


class Openshift(Kubernetes):

    _stats_available = Kubernetes._stats_available.copy()
    _stats_available.update({
        'num_route': lambda self: len(self.list_route()),
        'num_template': lambda self: len(self.list_template())
    })

    def __init__(self,
            hostname, protocol="https", port=8443, k_entry="api/v1", o_entry="oapi/v1", **kwargs):
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.token = kwargs.get('token', '')
        self.auth = self.token if self.token else (self.username, self.password)
        self.k_api = ContainerClient(hostname, self.auth, protocol, port, k_entry)
        self.o_api = ContainerClient(hostname, self.auth, protocol, port, o_entry)
        self.api = self.k_api  # default api is the kubernetes one for Kubernetes-class requests

    def list_route(self):
        """Returns list of routes"""
        entities = []
        entities_j = self.o_api.get('route')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Route(meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_service(self):
        """Returns list of services"""
        entities = []
        entities_j = self.api.get('service')[1]['items']
        for entity_j in entities_j:
            meta, spec = entity_j['metadata'], entity_j['spec']
            entity = Service(
                meta['name'], meta['namespace'], spec['clusterIP'], spec['sessionAffinity'])
            entities.append(entity)
        return entities

    def list_docker_registry(self):
        """Returns IP and port of the docker registry"""
        entities = []
        entities_j = self.o_api.get('imagestream')[1]['items']
        for entity_j in entities_j:
            if 'dockerImageRepository' not in entity_j['status']:
                continue
            reg_raw = entity_j['status']['dockerImageRepository'].split('/')[0]
            host, port = reg_raw.split(':') if ':' in reg_raw else (reg_raw, '')
            entity = ImageRegistry(host, port)
            if entity not in entities:
                entities.append(entity)
        return entities

    def list_project(self):
        """Returns list of projects"""
        entities = []
        entities_j = self.o_api.get('project')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Project(meta['name'])
            entities.append(entity)
        return entities

    def list_template(self):
        """Returns list of templates"""
        entities = []
        entities_j = self.o_api.get('template')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Template(meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_image_openshift(self):
        entities = []
        entities_j = self.o_api.get('image')[1]['items']
        for entity_j in entities_j:
            entities.append(entity_j['metadata'])
        return entities
