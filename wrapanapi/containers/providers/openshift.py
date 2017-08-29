from wrapanapi.containers.providers.kubernetes import Kubernetes
from wrapanapi.rest_client import ContainerClient

from wrapanapi.containers.route import Route
from wrapanapi.containers.image_registry import ImageRegistry
from wrapanapi.containers.project import Project
from wrapanapi.containers.template import Template
from wrapanapi.containers.image import Image
from wrapanapi.containers.deployment_config import DeploymentConfig

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
        self.list_image_openshift = self.list_docker_image  # For backward compatibility

    def list_route(self):
        """Returns list of routes"""
        entities = []
        entities_j = self.o_api.get('route')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Route(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_docker_registry(self):
        """Returns list of docker registries"""
        entities = []
        entities_j = self.o_api.get('imagestream')[1]['items']
        for entity_j in entities_j:
            if 'dockerImageRepository' not in entity_j['status']:
                continue
            meta = entity_j['metadata']
            entity = ImageRegistry(self, meta['name'],
                                   entity_j['status']['dockerImageRepository'],
                                   meta['namespace'])
            if entity not in entities:
                entities.append(entity)
        return entities

    def list_project(self):
        """Returns list of projects"""
        entities = []
        entities_j = self.o_api.get('project')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Project(self, meta['name'])
            entities.append(entity)
        return entities

    def list_template(self):
        """Returns list of templates"""
        entities = []
        entities_j = self.o_api.get('template')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Template(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_docker_image(self):
        """Returns list of images (Docker registry only)"""
        entities = []
        entities_j = self.o_api.get('image')[1]['items']
        for entity_j in entities_j:
            if 'dockerImageReference' not in entity_j:
                continue
            _, name, image_id, _ = Image.parse_docker_image_info(entity_j['dockerImageReference'])
            entities.append(Image(self, name, image_id))
        return entities

    def list_deployment_config(self):
        """Returns list of deployment configs"""
        entities = []
        entities_j = self.o_api.get('deploymentconfig')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = DeploymentConfig(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities
