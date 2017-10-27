from wrapanapi.base import WrapanapiAPIBase
from wrapanapi.rest_client import ContainerClient

from wrapanapi.containers.container import Container
from wrapanapi.containers.pod import Pod
from wrapanapi.containers.service import Service
from wrapanapi.containers.replicator import Replicator
from wrapanapi.containers.image import Image
from wrapanapi.containers.node import Node
from wrapanapi.containers.image_registry import ImageRegistry
from wrapanapi.containers.project import Project
from wrapanapi.containers.volume import Volume

"""
Related yaml structures:

[cfme_data]
management_systems:
    kubernetes:
        name: My kubernetes
        type: kubernetes
        hostname: 10.12.13.14
        port: 6443
        credentials: kubernetes
        authenticate: true
        rest_protocol: https

[credentials]
kubernetes:
    username: admin
    password: secret
    token: mytoken
"""


class Kubernetes(WrapanapiAPIBase):

    _stats_available = {
        'num_container': lambda self: len(self.list_container()),
        'num_pod': lambda self: len(self.list_container_group()),
        'num_service': lambda self: len(self.list_service()),
        'num_replication_controller':
            lambda self: len(self.list_replication_controller()),
        'num_replication_controller_labels':
            lambda self: len(self.list_replication_controller_labels()),
        'num_image': lambda self: len(self.list_image()),
        'num_node': lambda self: len(self.list_node()),
        'num_image_registry': lambda self: len(self.list_image_registry()),
        'num_project': lambda self: len(self.list_project()),
    }

    def __init__(self, hostname, protocol="https", port=6443, entry='api/v1', **kwargs):
        self.hostname = hostname
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.token = kwargs.get('token', '')
        self.auth = self.token if self.token else (self.username, self.password)
        self.api = ContainerClient(hostname, self.auth, protocol, port, entry)

    def disconnect(self):
        pass

    def _parse_image_info(self, image_str):
        """Splits full image name into registry, name and tag

        Both registry and tag are optional, name is always present.

        Example:
            localhost:5000/nginx:latest => localhost:5000, nginx, latest
        """
        registry, image_str = image_str.split('/', 1) if '/' in image_str else ('', image_str)
        name, tag = image_str.split(':', 1) if ':' in image_str else (image_str, '')
        return registry, name, tag

    def info(self):
        """Returns information about the cluster - number of CPUs and memory in GB"""
        aggregate_cpu, aggregate_mem = 0, 0
        for node in self.list_node():
            aggregate_cpu += node.cpu
            aggregate_mem += node.memory
        return {'cpu': aggregate_cpu, 'memory': aggregate_mem}

    def list_container(self):
        """Returns list of containers (derived from pods)"""
        entities = []
        entities_j = self.api.get('pod')[1]['items']
        for entity_j in entities_j:
            pod = Pod(self, entity_j['metadata']['name'], entity_j['metadata']['namespace'])
            conts_j = entity_j['spec']['containers']
            for cont_j in conts_j:
                cont = Container(self, cont_j['name'], pod, cont_j['image'])
                if cont not in entities:
                    entities.append(cont)
        return entities

    def list_container_group(self):
        """Returns list of container groups (pods)"""
        entities = []
        entities_j = self.api.get('pod')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Pod(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_service(self):
        """Returns list of services"""
        entities = []
        entities_j = self.api.get('service')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Service(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_replication_controller(self):
        """Returns list of replication controllers"""
        entities = []
        entities_j = self.api.get('replicationcontroller')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Replicator(self, meta['name'], meta['namespace'])
            entities.append(entity)
        return entities

    def list_image(self):
        """Returns list of images (derived from pods)"""
        entities = []
        entities_j = self.api.get('pod')[1]['items']
        for entity_j in entities_j:
            imgs_j = entity_j['status'].get('containerStatuses', [])
            for img_j in imgs_j:
                _, name, _ = self._parse_image_info(img_j['image'])
                img = Image(self, name, img_j['imageID'])
                if img not in entities:
                    entities.append(img)
        return entities

    def list_node(self):
        """Returns list of nodes"""
        entities = []
        entities_j = self.api.get('node')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Node(self, meta['name'])
            entities.append(entity)
        return entities

    def list_image_registry(self):
        """Returns list of image registries (derived from pods)"""
        entities = []
        entities_j = self.api.get('pod')[1]['items']
        for entity_j in entities_j:
            imgs_j = entity_j['status'].get('containerStatuses', [])
            for img_j in imgs_j:
                registry, _, _ = self._parse_image_info(img_j['image'])
                if not registry:
                    continue
                host, _ = registry.split(':') if ':' in registry else (registry, '')
                entity = ImageRegistry(self, host, registry, None)
                if entity not in entities:
                    entities.append(entity)
        return entities

    def list_project(self):
        """Returns list of projects (namespaces in k8s)"""
        entities = []
        entities_j = self.api.get('namespace')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Project(self, meta['name'])
            entities.append(entity)
        return entities

    def list_volume(self):
        entities = []
        entities_j = self.api.get('persistentvolume')[1]['items']
        for entity_j in entities_j:
            meta = entity_j['metadata']
            entity = Volume(self, meta['name'])
            entities.append(entity)
        return entities
